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
package brave.cassandra;

import brave.cassandra.driver.CassandraClientTracing;
import brave.cassandra.driver.TracingSession;
import brave.propagation.CurrentTraceContext.Scope;
import brave.test.ITRemote;
import cassandra.CassandraRule;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.function.Function;
import org.junit.ClassRule;
import org.junit.Test;

import static brave.Span.Kind.CLIENT;
import static brave.Span.Kind.SERVER;
import static brave.propagation.SamplingFlags.NOT_SAMPLED;
import static org.assertj.core.api.Assertions.assertThat;

public class ITTracing extends ITRemote {
  static {
    System.setProperty("cassandra.custom_tracing_class", Tracing.class.getName());
  }

  @ClassRule public static CassandraRule cassandra = new CassandraRule();

  @Test public void doesntTraceWhenTracingDisabled() {
    execute(session -> session.prepare("SELECT * from system.schema_keyspaces").bind());
  }

  @Test public void startsNewTraceWhenTracingEnabled() {
    execute(session -> session
        .prepare("SELECT * from system.schema_keyspaces")
        .enableTracing()
        .setOutgoingPayload(new LinkedHashMap<>())
        .bind());

    testSpanHandler.takeRemoteSpan(SERVER);
  }

  @Test public void startsNewTraceWhenTracingEnabled_noPayload() {
    execute(session -> session
        .prepare("SELECT * from system.schema_keyspaces").enableTracing().bind());

    testSpanHandler.takeRemoteSpan(SERVER);
  }

  @Test public void samplingDisabled() {
    try (Scope unsampled = currentTraceContext.newScope(newTraceContext(NOT_SAMPLED))) {
      executeTraced(session -> session.prepare("SELECT * from system.schema_keyspaces").bind());
    }

    // test rule ensures no span was sampled
  }

  @Test public void usesExistingTraceId() {
    executeTraced(session -> session
        .prepare("SELECT * from system.schema_keyspaces").bind());

    testSpanHandler.takeRemoteSpan(SERVER);
    testSpanHandler.takeRemoteSpan(CLIENT);
  }

  @Test public void reportsServerKindToZipkin() {
    execute(session -> session
        .prepare("SELECT * from system.schema_keyspaces").enableTracing().bind());

    testSpanHandler.takeRemoteSpan(SERVER);
  }

  @Test public void defaultSpanNameIsType() {
    execute(session -> session
        .prepare("SELECT * from system.schema_keyspaces").enableTracing().bind());

    assertThat(testSpanHandler.takeRemoteSpan(SERVER).name())
        .isEqualTo("QUERY");
  }

  @Test public void defaultRequestTags() {
    execute(session -> session
        .prepare("SELECT * from system.schema_keyspaces").enableTracing().bind());

    assertThat(testSpanHandler.takeRemoteSpan(SERVER).tags())
        .containsOnlyKeys("cassandra.request", "cassandra.session_id");
  }

  @Test public void reportsClientAddress() {
    execute(session -> session
        .prepare("SELECT * from system.schema_keyspaces").enableTracing().bind());

    assertThat(testSpanHandler.takeRemoteSpan(SERVER).remoteIp())
        .isNotNull();
  }

  void execute(Function<Session, BoundStatement> statement) {
    try (Cluster cluster = Cluster.builder()
        .addContactPointsWithPorts(Collections.singleton(cassandra.contactPoint()))
        .build(); Session session = cluster.connect()) {
      session.execute(statement.apply(session));
    }
  }

  void executeTraced(Function<Session, Statement> statement) {
    CassandraClientTracing withPropagation = CassandraClientTracing.newBuilder(tracing)
        .propagationEnabled(true).build();
    try (Cluster cluster = Cluster.builder()
        .addContactPointsWithPorts(Collections.singleton(cassandra.contactPoint()))
        .build(); Session session = TracingSession.create(withPropagation, cluster.connect())) {
      session.execute(statement.apply(session));
    }
  }
}
