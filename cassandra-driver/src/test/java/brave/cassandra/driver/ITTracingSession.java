/**
 * Copyright 2017 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package brave.cassandra.driver;

import brave.SpanCustomizer;
import brave.Tracer;
import brave.Tracing;
import brave.propagation.StrictCurrentTraceContext;
import brave.sampler.Sampler;
import cassandra.CassandraRule;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import zipkin2.Annotation;
import zipkin2.Endpoint;
import zipkin2.Span;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;
import static org.junit.Assume.assumeTrue;

public class ITTracingSession {
  static {
    System.setProperty("cassandra.custom_tracing_class", CustomPayloadCaptor.class.getName());
  }

  @ClassRule public static CassandraRule cassandra = new CassandraRule();

  ConcurrentLinkedDeque<Span> spans = new ConcurrentLinkedDeque<>();

  Tracing tracing;
  CassandraClientTracing cassandraTracing;
  Cluster cluster;
  Session session;
  PreparedStatement prepared;

  @Before public void setup() throws IOException {
    tracing = tracingBuilder(Sampler.ALWAYS_SAMPLE).build();
    cluster = Cluster.builder()
        .addContactPointsWithPorts(Collections.singleton(cassandra.contactPoint()))
        .build();
    session = newSession();
    CustomPayloadCaptor.ref.set(null);
  }

  @After public void close() throws Exception {
    if (session != null) session.close();
    if (cluster != null) cluster.close();
    if (tracing != null) tracing.close();
  }

  Session newSession() {
    cassandraTracing = CassandraClientTracing.create(tracing);
    Session result = TracingSession.create(cassandraTracing, cluster.connect());
    prepared = result.prepare("SELECT * from system.schema_keyspaces");
    return result;
  }

  @Test public void makesChildOfCurrentSpan() throws Exception {
    brave.Span parent = tracing.tracer().newTrace().name("test").start();
    try (Tracer.SpanInScope ws = tracing.tracer().withSpanInScope(parent)) {
      invokeBoundStatement();
    } finally {
      parent.finish();
    }

    assertThat(spans)
        .extracting(Span::traceId)
        .contains(parent.context().traceIdString());
  }

  // CASSANDRA-12835 particularly is in 3.11, which fixes simple (non-bound) statement tracing
  @Test public void propagatesTraceIds_regularStatement() throws Exception {
    session.execute("SELECT * from system.schema_keyspaces");
    assertThat(CustomPayloadCaptor.ref.get())
        .isEmpty();
  }

  @Test public void propagatesTraceIds() throws Exception {
    invokeBoundStatement();

    assertThat(CustomPayloadCaptor.ref.get().keySet())
        .containsExactly("X-B3-SpanId", "X-B3-Sampled", "X-B3-TraceId");
  }

  @Test public void propagatesSampledFalse() throws Exception {
    tracing = tracingBuilder(Sampler.NEVER_SAMPLE).build();
    session.close();
    session = newSession();
    invokeBoundStatement();

    assertThat(CustomPayloadCaptor.ref.get().get("X-B3-Sampled"))
        .extracting(ByteBuffer::get)
        .containsExactly('0');
  }

  @Test public void reportsClientAnnotationsToZipkin() throws Exception {
    invokeBoundStatement();

    assertThat(spans)
        .flatExtracting(Span::annotations)
        .extracting(Annotation::value)
        .containsExactly("cs", "cr");
  }

  @Test public void defaultSpanNameIsQuery() throws Exception {
    invokeBoundStatement();

    assertThat(spans)
        .extracting(Span::name)
        .containsExactly("bound-statement");
  }

  @Test public void reportsSpanOnTransportException() throws Exception {
    cluster.close();

    try {
      invokeBoundStatement();
      failBecauseExceptionWasNotThrown(NoHostAvailableException.class);
    } catch (NoHostAvailableException e) {
    }

    assertThat(spans).hasSize(1);
  }

  @Test public void addsErrorTag_onTransportException() throws Exception {
    reportsSpanOnTransportException();

    assertThat(spans)
        .flatExtracting(s -> s.tags().entrySet())
        .containsOnlyOnce(entry("error", "All host(s) tried for query failed (no host was tried)"));
  }

  @Test public void addsErrorTag_onCanceledFuture() throws Exception {
    ResultSetFuture resp = session.executeAsync("SELECT * from system.schema_keyspaces");
    assumeTrue("lost race on cancel", resp.cancel(true));

    close(); // blocks until the cancel finished

    assertThat(spans).flatExtracting(s -> s.tags().entrySet())
        .containsOnlyOnce(entry("error", "Task was cancelled."));
  }

  @Test public void reportsServerAddress() throws Exception {
    invokeBoundStatement();

    assertThat(spans)
        .flatExtracting(Span::remoteEndpoint)
        .containsExactly(Endpoint.newBuilder()
            .serviceName(cluster.getClusterName())
            .ip("127.0.0.1")
            .port(cassandra.contactPoint().getPort()).build()
        );
  }

  @Test public void customSampler() throws Exception {
    cassandraTracing = cassandraTracing.toBuilder()
        .sampler(CassandraClientSampler.NEVER_SAMPLE).build();
    session = TracingSession.create(cassandraTracing, ((TracingSession) session).delegate);

    invokeBoundStatement();

    assertThat(spans).isEmpty();
  }

  @Test public void supportsCustomization() throws Exception {
    cassandraTracing = cassandraTracing.toBuilder()
        .parser(new CassandraClientParser() {
          @Override public String spanName(Statement statement) {
            return "query";
          }

          @Override public void request(Statement statement, SpanCustomizer customizer) {
            super.request(statement, customizer);
            customizer.tag("cassandra.fetch_size", Integer.toString(statement.getFetchSize()));
          }

          @Override public void response(ResultSet resultSet, SpanCustomizer customizer) {
            customizer.tag("cassandra.available_without_fetching",
                Integer.toString(resultSet.getAvailableWithoutFetching()));
          }
        })
        .build().clientOf("remote-cluster");
    session = TracingSession.create(cassandraTracing, ((TracingSession) session).delegate);

    invokeBoundStatement();

    assertThat(spans)
        .extracting(Span::name)
        .containsExactly("query");

    assertThat(spans)
        .flatExtracting(Span::remoteServiceName)
        .containsExactly("remote-cluster");
  }

  void invokeBoundStatement() {
    session.execute(prepared.bind());
  }

  Tracing.Builder tracingBuilder(Sampler sampler) {
    return brave.Tracing.newBuilder()
        .currentTraceContext(new StrictCurrentTraceContext())
        .spanReporter(spans::add)
        .sampler(sampler);
  }
}
