/*
 * Copyright 2017-2020 The OpenZipkin Authors
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
import brave.handler.MutableSpan;
import brave.internal.Nullable;
import brave.propagation.B3SingleFormat;
import brave.propagation.CurrentTraceContext.Scope;
import brave.propagation.SamplingFlags;
import brave.propagation.TraceContext;
import brave.test.ITRemote;
import cassandra.CassandraContainer;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverInternalError;
import java.nio.ByteBuffer;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static brave.Span.Kind.CLIENT;
import static brave.propagation.SamplingFlags.NOT_SAMPLED;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.spy;

public class ITTracingSession extends ITRemote {
  // Takes a while to startup, so we make this a class rule
  @ClassRule public static CassandraContainer cassandra = new CassandraContainer();

  CassandraClientTracing cassandraTracing;
  Cluster cluster;
  Session session, spiedSession;
  PreparedStatement prepared;

  @Before public void setup() {
    cluster = Cluster.builder().addContactPointsWithPorts(cassandra.contactPoint()).build();
    session = newSession(CassandraClientTracing.newBuilder(tracing).propagationEnabled(true));
  }

  @After public void close() {
    if (cluster != null) cluster.close();
  }

  Session newSession(CassandraClientTracing.Builder builder) {
    cassandraTracing = builder.build();
    spiedSession = spy(cluster.connect());
    Session result = TracingSession.create(cassandraTracing, spiedSession);
    prepared = result.prepare("SELECT release_version from system.local");
    return result;
  }

  @Test public void makesChildOfCurrentSpan() {
    TraceContext parent = newTraceContext(SamplingFlags.SAMPLED);
    try (Scope scope = currentTraceContext.newScope(parent)) {
      invokeBoundStatement();
    }

    TraceContext extracted = extractB3Header();
    assertThat(extracted.sampled()).isTrue();
    assertChildOf(extracted, parent);
    assertSameIds(testSpanHandler.takeRemoteSpan(CLIENT), extracted);
  }

  // CASSANDRA-12835 particularly is in 3.11, which fixes simple (non-bound) statement tracing
  @Test public void propagatesTraceIds_regularStatement() {
    session.execute("SELECT release_version from system.local");

    TraceContext extracted = extractB3Header();
    assertThat(extracted.sampled()).isTrue();
    assertSameIds(testSpanHandler.takeRemoteSpan(CLIENT), extracted);
  }

  @Test public void propagatesTraceIds() {
    invokeBoundStatement();

    TraceContext extracted = extractB3Header();
    assertThat(extracted.sampled()).isTrue();
    assertSameIds(testSpanHandler.takeRemoteSpan(CLIENT), extracted);
  }

  @Test public void propagationDisabledByDefault() {
    session.close();
    session = newSession(CassandraClientTracing.newBuilder(tracing));

    invokeBoundStatement();

    // span was reported
    testSpanHandler.takeRemoteSpan(CLIENT);
    // but nothing was propagated
    assertThat(extractB3Header()).isNull();
  }

  @Test public void propagatesSampledFalse() {
    try (Scope unsampled = currentTraceContext.newScope(newTraceContext(NOT_SAMPLED))) {
      invokeBoundStatement();
    }

    TraceContext extracted = extractB3Header();
    assertThat(extracted.sampled()).isFalse();

    // test rule ensures no span was sampled
  }

  @Test public void reportsClientKindToZipkin() {
    invokeBoundStatement();

    testSpanHandler.takeRemoteSpan(CLIENT);
  }

  @Test public void defaultSpanNameIsQuery() {
    invokeBoundStatement();

    assertThat(testSpanHandler.takeRemoteSpan(CLIENT).name())
        .isEqualTo("bound-statement");
  }

  @Test public void reportsSpanOnTransportException() {
    cluster.close();

    assertThatThrownBy(this::invokeBoundStatement)
        .isInstanceOf(DriverInternalError.class);

    testSpanHandler.takeRemoteSpanWithErrorMessage(CLIENT,
        "Could not send request, session is closed");
  }

  @Test public void addsErrorTag_onCanceledFuture() {
    ResultSetFuture resp = session.executeAsync("SELECT release_version from system.local");
    assumeTrue("lost race on cancel", resp.cancel(true));

    close(); // blocks until the cancel finished

    testSpanHandler.takeRemoteSpanWithErrorMessage(CLIENT, "Task was cancelled.");
  }

  @Test public void reportsServerAddress() {
    invokeBoundStatement();

    MutableSpan span = testSpanHandler.takeRemoteSpan(CLIENT);
    assertThat(span.remoteServiceName()).isEqualTo(cluster.getClusterName());
    assertThat(span.remoteIp()).isEqualTo("127.0.0.1");
    assertThat(span.remotePort()).isEqualTo(cassandra.contactPoint().getPort());
  }

  @Test public void customSampler() {
    cassandraTracing =
        cassandraTracing.toBuilder().sampler(CassandraClientSampler.NEVER_SAMPLE).build();
    session = TracingSession.create(cassandraTracing, ((TracingSession) session).delegate);

    invokeBoundStatement();

    // test rule ensures no span was sampled
  }

  @Test public void supportsCustomization() {
    cassandraTracing = cassandraTracing.toBuilder().parser(new CassandraClientParser() {
      @Override public String spanName(Statement statement) {
        return "query";
      }

      @Override public void request(Statement statement, SpanCustomizer customizer) {
        super.request(statement, customizer);
        customizer.tag(
            "cassandra.fetch_size", Integer.toString(statement.getFetchSize()));
      }

      @Override public void response(ResultSet resultSet, SpanCustomizer customizer) {
        customizer.tag(
            "cassandra.available_without_fetching",
            Integer.toString(resultSet.getAvailableWithoutFetching()));
      }
    }).build().clientOf("remote-cluster");
    session = TracingSession.create(cassandraTracing, ((TracingSession) session).delegate);

    invokeBoundStatement();

    MutableSpan span = testSpanHandler.takeRemoteSpan(CLIENT);
    assertThat(span.name()).isEqualTo("query");
    assertThat(span.remoteServiceName()).isEqualTo("remote-cluster");
  }

  void invokeBoundStatement() {
    session.execute(prepared.bind());
  }

  @Nullable TraceContext extractB3Header() {
    ArgumentCaptor<Statement> captor = ArgumentCaptor.forClass(Statement.class);
    Mockito.verify(spiedSession).executeAsync(captor.capture());
    Map<String, ByteBuffer> payload = captor.getValue().getOutgoingPayload();
    if (payload == null || payload.get("b3") == null) return null;
    return B3SingleFormat.parseB3SingleFormat(UTF_8.decode(payload.get("b3"))).context();
  }
}
