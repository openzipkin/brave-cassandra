/*
 * Copyright 2017-2019 The OpenZipkin Authors
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
import brave.propagation.B3SingleFormat;
import brave.propagation.StrictScopeDecorator;
import brave.propagation.ThreadLocalCurrentTraceContext;
import brave.sampler.Sampler;
import cassandra.CassandraRule;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverInternalError;
import java.util.Collections;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import zipkin2.Endpoint;
import zipkin2.Span;

import static java.nio.charset.StandardCharsets.UTF_8;
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

  @Before
  public void setup() {
    tracing = tracingBuilder(Sampler.ALWAYS_SAMPLE).build();
    cluster =
        Cluster.builder()
            .addContactPointsWithPorts(Collections.singleton(cassandra.contactPoint()))
            .build();
    session = newSession();
    CustomPayloadCaptor.ref.set(null);
  }

  @After
  public void close() {
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

  @Test
  public void makesChildOfCurrentSpan() {
    brave.Span parent = tracing.tracer().newTrace().name("test").start();
    try (Tracer.SpanInScope ws = tracing.tracer().withSpanInScope(parent)) {
      invokeBoundStatement();
    } finally {
      parent.finish();
    }

    assertThat(spans).extracting(Span::traceId).contains(parent.context().traceIdString());
  }

  // CASSANDRA-12835 particularly is in 3.11, which fixes simple (non-bound) statement tracing
  @Test
  public void propagatesTraceIds_regularStatement() {
    cassandraTracing = CassandraClientTracing.newBuilder(tracing).propagationEnabled(true).build();
    session.close();
    session = TracingSession.create(cassandraTracing, cluster.connect());

    session.execute("SELECT * from system.schema_keyspaces");

    assertThat(CustomPayloadCaptor.ref.get().keySet()).containsOnly("b3");
  }

  @Test
  public void propagatesTraceIds() {
    cassandraTracing = CassandraClientTracing.newBuilder(tracing).propagationEnabled(true).build();
    session.close();
    session = TracingSession.create(cassandraTracing, cluster.connect());

    invokeBoundStatement();

    assertThat(CustomPayloadCaptor.ref.get().keySet()).containsOnly("b3");
  }

  @Test
  public void propagationDisabledByDefault() {
    invokeBoundStatement();

    assertThat(CustomPayloadCaptor.ref.get())
        .isNull();
  }

  @Test
  public void propagatesSampledFalse() {
    tracing = tracingBuilder(Sampler.NEVER_SAMPLE).build();
    cassandraTracing = CassandraClientTracing.newBuilder(tracing).propagationEnabled(true).build();

    session.close();
    session = TracingSession.create(cassandraTracing, cluster.connect());
    prepared = session.prepare("SELECT * from system.schema_keyspaces");

    invokeBoundStatement();

    assertThat(CustomPayloadCaptor.ref.get().get("b3"))
        .extracting(b -> B3SingleFormat.parseB3SingleFormat(UTF_8.decode(b)).sampled())
        .isEqualTo(Boolean.FALSE);
  }

  @Test
  public void reportsClientKindToZipkin() {
    invokeBoundStatement();

    assertThat(spans).flatExtracting(Span::kind).containsExactly(Span.Kind.CLIENT);
  }

  @Test
  public void defaultSpanNameIsQuery() {
    invokeBoundStatement();

    assertThat(spans).extracting(Span::name).containsExactly("bound-statement");
  }

  @Test
  public void reportsSpanOnTransportException() {
    cluster.close();

    try {
      invokeBoundStatement();
      failBecauseExceptionWasNotThrown(DriverInternalError.class);
    } catch (DriverInternalError e) {
    }

    assertThat(spans).hasSize(1);
  }

  @Test
  public void addsErrorTag_onTransportException() {
    reportsSpanOnTransportException();

    assertThat(spans)
        .flatExtracting(s -> s.tags().entrySet())
        .containsOnlyOnce(entry("error", "Could not send request, session is closed"));
  }

  @Test
  public void addsErrorTag_onCanceledFuture() {
    ResultSetFuture resp = session.executeAsync("SELECT * from system.schema_keyspaces");
    assumeTrue("lost race on cancel", resp.cancel(true));

    close(); // blocks until the cancel finished

    assertThat(spans)
        .flatExtracting(s -> s.tags().entrySet())
        .containsOnlyOnce(entry("error", "Task was cancelled."));
  }

  @Test
  public void reportsServerAddress() {
    invokeBoundStatement();

    assertThat(spans)
        .flatExtracting(Span::remoteEndpoint)
        .containsExactly(
            Endpoint.newBuilder()
                .serviceName(cluster.getClusterName())
                .ip("127.0.0.1")
                .port(cassandra.contactPoint().getPort())
                .build());
  }

  @Test
  public void customSampler() {
    cassandraTracing =
        cassandraTracing.toBuilder().sampler(CassandraClientSampler.NEVER_SAMPLE).build();
    session = TracingSession.create(cassandraTracing, ((TracingSession) session).delegate);

    invokeBoundStatement();

    assertThat(spans).isEmpty();
  }

  @Test
  public void supportsCustomization() {
    cassandraTracing =
        cassandraTracing
            .toBuilder()
            .parser(
                new CassandraClientParser() {
                  @Override
                  public String spanName(Statement statement) {
                    return "query";
                  }

                  @Override
                  public void request(Statement statement, SpanCustomizer customizer) {
                    super.request(statement, customizer);
                    customizer.tag(
                        "cassandra.fetch_size", Integer.toString(statement.getFetchSize()));
                  }

                  @Override
                  public void response(ResultSet resultSet, SpanCustomizer customizer) {
                    customizer.tag(
                        "cassandra.available_without_fetching",
                        Integer.toString(resultSet.getAvailableWithoutFetching()));
                  }
                })
            .build()
            .clientOf("remote-cluster");
    session = TracingSession.create(cassandraTracing, ((TracingSession) session).delegate);

    invokeBoundStatement();

    assertThat(spans).extracting(Span::name).containsExactly("query");

    assertThat(spans).flatExtracting(Span::remoteServiceName).containsExactly("remote-cluster");
  }

  void invokeBoundStatement() {
    session.execute(prepared.bind());
  }

  Tracing.Builder tracingBuilder(Sampler sampler) {
    return brave.Tracing.newBuilder()
        .currentTraceContext(ThreadLocalCurrentTraceContext.newBuilder()
            .addScopeDecorator(StrictScopeDecorator.create())
            .build())
        .spanReporter(spans::add)
        .sampler(sampler);
  }
}
