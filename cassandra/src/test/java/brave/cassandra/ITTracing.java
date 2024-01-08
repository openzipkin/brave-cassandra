/*
 * Copyright 2017-2024 The OpenZipkin Authors
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

import brave.handler.SpanHandler;
import brave.propagation.TraceContext;
import brave.test.ITRemote;
import cassandra.CassandraContainer;
import cassandra.ForwardHttpSpansToHandler;
import cassandra.ForwardingSpanHandler;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;
import zipkin2.reporter.Reporter;
import zipkin2.reporter.brave.ZipkinSpanHandler;
import zipkin2.reporter.urlconnection.URLConnectionSender;

import static brave.Span.Kind.SERVER;
import static brave.propagation.B3SingleFormat.writeB3SingleFormatAsBytes;
import static brave.propagation.SamplingFlags.SAMPLED;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers(disabledWithoutDocker = true)
@Tag("docker")
class ITTracing extends ITRemote {
  // swap references on each method instead of starting Cassandra for each test
  static SpanHandler currentSpanHandler = SpanHandler.NOOP;

  @BeforeEach public void setCurrentSpanHandler() {
    currentSpanHandler = testSpanHandler;
  }

  @RegisterExtension public static ForwardHttpSpansToHandler zipkin =
      new ForwardHttpSpansToHandler(new ForwardingSpanHandler() {
        @Override protected SpanHandler delegate() {
          return currentSpanHandler;
        }
      });

  @Container CassandraContainer cassandra = copyTracingLibs(new CassandraContainer()
      .withEnv("LOGGING_LEVEL", "WARN")
      .withEnv("JAVA_OPTS", javaOpts(zipkin.httpPort()))
  );

  @Test void doesntTraceWhenTracingDisabled() {
    execute(session -> session.prepare("SELECT release_version from system.local").bind());
  }

  @Test void startsNewTraceWhenTracingEnabled() {
    execute(session -> session
        .prepare("SELECT release_version from system.local")
        .enableTracing()
        .setOutgoingPayload(new LinkedHashMap<>())
        .bind());

    testSpanHandler.takeRemoteSpan(SERVER);
  }

  @Test void startsNewTraceWhenTracingEnabled_noPayload() {
    execute(session -> session
        .prepare("SELECT release_version from system.local").enableTracing().bind());

    testSpanHandler.takeRemoteSpan(SERVER);
  }

  @Test void samplingDisabled() {
    execute(session -> session
        .prepare("SELECT release_version from system.local")
        .setOutgoingPayload(singletonMap("b3", ByteBuffer.wrap(new byte[] {'0'})))
        .bind());

    // test rule ensures no span was sampled
  }

  @Test void usesExistingTraceId() {
    TraceContext context = newTraceContext(SAMPLED);
    execute(session -> session
        .prepare("SELECT release_version from system.local")
        .enableTracing()
        .setOutgoingPayload(
            singletonMap("b3", ByteBuffer.wrap(writeB3SingleFormatAsBytes(context))))
        .bind());

    assertChildOf(testSpanHandler.takeRemoteSpan(SERVER), context);
  }

  @Test void reportsServerKindToZipkin() {
    execute(session -> session
        .prepare("SELECT release_version from system.local").enableTracing().bind());

    testSpanHandler.takeRemoteSpan(SERVER);
  }

  @Test void defaultSpanNameIsType() {
    execute(session -> session
        .prepare("SELECT release_version from system.local").enableTracing().bind());

    assertThat(testSpanHandler.takeRemoteSpan(SERVER).name())
        .isEqualTo("query");
  }

  @Test void defaultRequestTags() {
    execute(session -> session
        .prepare("SELECT release_version from system.local").enableTracing().bind());

    assertThat(testSpanHandler.takeRemoteSpan(SERVER).tags())
        .containsOnlyKeys("cassandra.request", "cassandra.session_id");
  }

  @Test void reportsClientAddress() {
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

  static <C extends GenericContainer<C>> C copyTracingLibs(C container) {
    // First detect if we are in an IDE or failsafe. The latter will see our shaded jar.
    String tracingPath =
        Tracing.class.getProtectionDomain().getCodeSource().getLocation().getFile();
    if (tracingPath.contains("brave-instrumentation-cassandra")) {
      if (!tracingPath.endsWith("-all.jar")) { // then switch to it
        tracingPath = tracingPath.replace(".jar", "-all.jar");
      }
      if (!new File(tracingPath).exists()) {
        throw new AssertionError(tracingPath + " missing. Check maven-shade-plugin configuration");
      }
      MountableFile allJar = MountableFile.forHostPath(tracingPath);
      return container.withCopyFileToContainer(allJar,
          "/cassandra/lib/brave-instrumentation-cassandra-all.jar");
    }

    // Otherwise, we need references to our main classpath in Cassandra's classpath
    return container
        .withCopyFileToContainer(codeSource(Tracing.class), "/cassandra/classes/")
        .withCopyFileToContainer(codeSource(brave.Tracing.class), "/cassandra/lib/brave.jar")
        .withCopyFileToContainer(codeSource(ZipkinSpanHandler.class),
            "/cassandra/lib/zipkin-reporter-brave.jar")
        .withCopyFileToContainer(codeSource(URLConnectionSender.class),
            "/cassandra/lib/zipkin-sender-urlconnection.jar")
        .withCopyFileToContainer(codeSource(Reporter.class), "/cassandra/lib/zipkin-reporter.jar")
        .withCopyFileToContainer(codeSource(zipkin2.Span.class), "/cassandra/lib/zipkin.jar");
  }

  /** Returns a mountable file to a path that's usually a jar in the Maven local repo. */
  static MountableFile codeSource(Class<?> clazz) {
    return MountableFile.forHostPath(
        clazz.getProtectionDomain().getCodeSource().getLocation().getFile());
  }

  /** Overwrite JAVA_OPTS to enable tracing and point it at the test Zipkin endpoint */
  static String javaOpts(int zipkinHttpPort) {
    // TODO: would be nicer if Testcontainers.exposeHostPort(int) and returned the input
    // https://github.com/testcontainers/testcontainers-java/issues/3538
    org.testcontainers.Testcontainers.exposeHostPorts(zipkinHttpPort);
    String zipkinEndpoint =
        "http://host.testcontainers.internal:" + zipkinHttpPort + "/api/v2/spans";

    // We could read JAVA_OPTS from the image with DockerClient.inspectImageCmd, but that is a cure
    // worse than the disease. It implies ensuring the image is pulled which can instantiate Docker
    // when we intentionally skipped it via "docker.skip". Instead, we copy/paste.
    return "-Xms256m -Xmx256m -XX:+ExitOnOutOfMemoryError -Djava.net.preferIPv4Stack=true"
        + " -Dcassandra.custom_tracing_class=" + Tracing.class.getName()
        + " -Dzipkin.fail_fast=true" + " -Dzipkin.http_endpoint=" + zipkinEndpoint;
  }
}
