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

import brave.propagation.TraceContext;
import brave.test.ITRemote;
import cassandra.CassandraContainer;
import cassandra.ForwardHttpSpansToHandler;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.Timeout;
import org.testcontainers.Testcontainers;
import org.testcontainers.images.RemoteDockerImage;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.MountableFile;
import zipkin2.reporter.Reporter;
import zipkin2.reporter.brave.ZipkinSpanHandler;
import zipkin2.reporter.urlconnection.URLConnectionSender;

import static brave.Span.Kind.SERVER;
import static brave.propagation.B3SingleFormat.writeB3SingleFormatAsBytes;
import static brave.propagation.SamplingFlags.SAMPLED;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;

public class ITTracing extends ITRemote {
  /** Builds a Cassandra image that adds {@link Tracing} and its dependencies in the classpath. */
  static RemoteDockerImage imageWithBraveTracer = new RemoteDockerImage(imageWithBraveTracer());

  @Rule public ForwardHttpSpansToHandler zipkin = new ForwardHttpSpansToHandler(testSpanHandler);
  @Rule public CassandraContainer cassandra = new CassandraContainer(imageWithBraveTracer)
      .withEnv("LOGGING_LEVEL", "INFO")
      .withEnv("JAVA_OPTS", javaOpts(zipkin.httpPort()));

  public ITTracing() {
    globalTimeout = new DisableOnDebug(Timeout.seconds(120)); // Cassandra takes longer than 20s
  }

  @Test public void doesntTraceWhenTracingDisabled() {
    execute(session -> session.prepare("SELECT release_version from system.local").bind());
  }

  @Test public void startsNewTraceWhenTracingEnabled() {
    execute(session -> session
        .prepare("SELECT release_version from system.local")
        .enableTracing()
        .setOutgoingPayload(new LinkedHashMap<>())
        .bind());

    testSpanHandler.takeRemoteSpan(SERVER);
  }

  @Test public void startsNewTraceWhenTracingEnabled_noPayload() {
    execute(session -> session
        .prepare("SELECT release_version from system.local").enableTracing().bind());

    testSpanHandler.takeRemoteSpan(SERVER);
  }

  @Test public void samplingDisabled() {
    execute(session -> session
        .prepare("SELECT release_version from system.local")
        .setOutgoingPayload(singletonMap("b3", ByteBuffer.wrap(new byte[] {'0'})))
        .bind());

    // test rule ensures no span was sampled
  }

  @Test public void usesExistingTraceId() {
    TraceContext context = newTraceContext(SAMPLED);
    execute(session -> session
        .prepare("SELECT release_version from system.local")
        .enableTracing()
        .setOutgoingPayload(
            singletonMap("b3", ByteBuffer.wrap(writeB3SingleFormatAsBytes(context))))
        .bind());

    assertChildOf(testSpanHandler.takeRemoteSpan(SERVER), context);
  }

  @Test public void reportsServerKindToZipkin() {
    execute(session -> session
        .prepare("SELECT release_version from system.local").enableTracing().bind());

    testSpanHandler.takeRemoteSpan(SERVER);
  }

  @Test public void defaultSpanNameIsType() {
    execute(session -> session
        .prepare("SELECT release_version from system.local").enableTracing().bind());

    assertThat(testSpanHandler.takeRemoteSpan(SERVER).name())
        .isEqualTo("query");
  }

  @Test public void defaultRequestTags() {
    execute(session -> session
        .prepare("SELECT release_version from system.local").enableTracing().bind());

    assertThat(testSpanHandler.takeRemoteSpan(SERVER).tags())
        .containsOnlyKeys("cassandra.request", "cassandra.session_id");
  }

  @Test public void reportsClientAddress() {
    execute(session -> session
        .prepare("SELECT release_version from system.local").enableTracing().bind());

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

  static ImageFromDockerfile imageWithBraveTracer() {
    ImageFromDockerfile image = new ImageFromDockerfile("openzipkin/brave-cassandra:test");
    // First detect if we are in an IDE or failsafe. The latter will see our shaded jar.
    MountableFile tracingCodeSource = codeSource(Tracing.class);
    if (tracingCodeSource.getResolvedPath().contains("brave-instrumentation-cassandra")) {
      return image
          .withFileFromTransferable("brave-instrumentation-cassandra.jar", tracingCodeSource)
          .withDockerfileFromBuilder(
              builder -> builder.from(CassandraContainer.IMAGE_NAME.asCanonicalNameString())
                  .add("brave-instrumentation-cassandra.jar",
                      "lib/brave-instrumentation-cassandra.jar")
          );
    }

    // Otherwise, we need references to our main classpath in Cassandra's classpath
    return image
        .withFileFromTransferable("classes", tracingCodeSource)
        .withFileFromTransferable("brave.jar", codeSource(brave.Tracing.class))
        .withFileFromTransferable("zipkin-reporter-brave.jar", codeSource(ZipkinSpanHandler.class))
        .withFileFromTransferable("zipkin-sender-urlconnection.jar",
            codeSource(URLConnectionSender.class))
        .withFileFromTransferable("zipkin-reporter.jar", codeSource(Reporter.class))
        .withFileFromTransferable("zipkin.jar", codeSource(zipkin2.Span.class))
        .withDockerfileFromBuilder(
            builder -> builder.from(CassandraContainer.IMAGE_NAME.asCanonicalNameString())
                // Copy the above references to a new layer over Zipkin's Cassandra image
                .add("classes/", "classes/")
                .add("brave.jar", "lib/brave.jar")
                .add("zipkin-reporter-brave.jar", "lib/zipkin-reporter-brave.jar")
                .add("zipkin-sender-urlconnection.jar", "lib/zipkin-sender-urlconnection.jar")
                .add("zipkin-reporter.jar", "lib/zipkin-reporter.jar")
                .add("zipkin.jar", "lib/zipkin.jar")
        );
  }

  static MountableFile codeSource(Class<?> clazz) {
    return MountableFile.forHostPath(
        clazz.getProtectionDomain().getCodeSource().getLocation().getFile());
  }

  /** Overwrite JAVA_OPTS to enable tracing and point it at the test Zipkin endpoint */
  @NotNull private static String javaOpts(int zipkinHttpPort) {
    // TODO: would be nicer if Testcontainers.exposeHostPort(int) and returned the input
    // https://github.com/testcontainers/testcontainers-java/issues/3538
    Testcontainers.exposeHostPorts(zipkinHttpPort);
    String zipkinEndpoint =
        "http://host.testcontainers.internal:" + zipkinHttpPort + "/api/v2/spans";

    // TODO: read prior JAVA_OPTS from base layer
    return "-Xms256m -Xmx256m -XX:+ExitOnOutOfMemoryError -Djava.net.preferIPv4Stack=true"
        + " -Dcassandra.custom_tracing_class=" + Tracing.class.getName() + " -Dzipkin.fail_fast=true"
        + " -Dzipkin.http_endpoint=" + zipkinEndpoint;
  }
}
