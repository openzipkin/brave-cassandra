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
package brave.cassandra;

import brave.Tracer;
import brave.cassandra.driver.TracingSession;
import brave.propagation.SamplingFlags;
import cassandra.CassandraRule;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Function;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import zipkin2.Span;

import static org.assertj.core.api.Assertions.assertThat;

public class ITTracing {
  static {
    System.setProperty("cassandra.custom_tracing_class", Tracing.class.getName());
  }

  @ClassRule public static CassandraRule cassandra = new CassandraRule();

  ConcurrentLinkedDeque<Span> spans = new ConcurrentLinkedDeque<>();
  brave.Tracing tracing = brave.Tracing.newBuilder()
      .localServiceName("cassandra")
      .spanReporter(spans::add)
      .build();

  @After public void after() {
    tracing.close();
  }

  @Test public void doesntTraceWhenTracingDisabled() throws IOException {
    execute(session -> session
        .prepare("SELECT * from system.schema_keyspaces").bind());

    assertThat(spans).isEmpty();
  }

  @Test public void startsNewTraceWhenTracingEnabled() throws IOException {
    execute(session -> session
        .prepare("SELECT * from system.schema_keyspaces")
        .enableTracing().setOutgoingPayload(new LinkedHashMap<>()).bind());

    assertThat(spans).hasSize(1);
  }

  @Test public void startsNewTraceWhenTracingEnabled_noPayload() throws IOException {
    execute(session -> session
        .prepare("SELECT * from system.schema_keyspaces")
        .enableTracing().bind());

    assertThat(spans).hasSize(1);
  }

  @Test public void samplingDisabled() throws IOException {
    brave.Span unsampled = tracing.tracer().newTrace(SamplingFlags.NOT_SAMPLED);
    try (Tracer.SpanInScope ws = tracing.tracer().withSpanInScope(unsampled)) {
      executeTraced(session -> session
          .prepare("SELECT * from system.schema_keyspaces").bind());
    }

    assertThat(spans).isEmpty();
  }

  @Test public void usesExistingTraceId() throws Exception {
    executeTraced(session -> session
        .prepare("SELECT * from system.schema_keyspaces").bind());

    assertThat(spans)
        .flatExtracting(Span::kind)
        .containsOnlyOnce(Span.Kind.SERVER, Span.Kind.CLIENT);
  }

  @Test public void reportsServerKindToZipkin() throws Exception {
    execute(session -> session
        .prepare("SELECT * from system.schema_keyspaces")
        .enableTracing().bind());

    assertThat(spans)
        .flatExtracting(Span::kind)
        .containsOnlyOnce(Span.Kind.SERVER);
  }

  @Test public void defaultSpanNameIsType() throws Exception {
    execute(session -> session
        .prepare("SELECT * from system.schema_keyspaces")
        .enableTracing().bind());

    assertThat(spans)
        .extracting(Span::name)
        .containsExactly("query");
  }

  @Test public void defaultRequestTags() throws Exception {
    execute(session -> session
        .prepare("SELECT * from system.schema_keyspaces")
        .enableTracing().bind());

    assertThat(spans)
        .flatExtracting(s -> s.tags().keySet())
        .contains("cassandra.request", "cassandra.session_id");
  }

  @Test public void reportsClientAddress() throws Exception {
    execute(session -> session
        .prepare("SELECT * from system.schema_keyspaces")
        .enableTracing().bind());

    assertThat(spans)
        .flatExtracting(Span::remoteEndpoint)
        .hasSize(1)
        .doesNotContainNull();
  }

  void execute(Function<Session, BoundStatement> statement) {
    try (Cluster cluster = Cluster.builder()
        .addContactPointsWithPorts(Collections.singleton(cassandra.contactPoint()))
        .build(); Session session = cluster.connect()) {
      session.execute(statement.apply(session));
    }
  }

  void executeTraced(Function<Session, Statement> statement) {
    try (Cluster cluster = Cluster.builder()
        .addContactPointsWithPorts(Collections.singleton(cassandra.contactPoint()))
        .build(); Session session = TracingSession.create(tracing, cluster.connect())) {
      session.execute(statement.apply(session));
    }
  }
}
