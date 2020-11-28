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
package cassandra;

import java.net.InetSocketAddress;
import java.time.Duration;
import org.junit.AssumptionViolatedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.RemoteDockerImage;
import org.testcontainers.utility.DockerImageName;

import static org.testcontainers.utility.DockerImageName.parse;

// mostly waiting for https://github.com/testcontainers/testcontainers-java/issues/3537
public class CassandraContainer extends GenericContainer<CassandraContainer> {
  public static final DockerImageName IMAGE_NAME =
      parse("ghcr.io/openzipkin/zipkin-cassandra:2.23.0");
  static final int CASSANDRA_PORT = 9042;

  final Logger logger = LoggerFactory.getLogger(CassandraContainer.class);

  public CassandraContainer() {
    this(new RemoteDockerImage(IMAGE_NAME));
  }

  public CassandraContainer(RemoteDockerImage image) {
    super(image);
    if ("true".equals(System.getProperty("docker.skip"))) {
      throw new AssumptionViolatedException("${docker.skip} == true");
    }
    withExposedPorts(CASSANDRA_PORT);
    waitStrategy = Wait.forHealthcheck();
    withStartupTimeout(Duration.ofMinutes(2));
    withLogConsumer(new Slf4jLogConsumer(logger));
  }

  public InetSocketAddress contactPoint() {
    if (!isRunning()) throw new AssumptionViolatedException("Container isn't running");
    return new InetSocketAddress(getContainerIpAddress(), getMappedPort(CASSANDRA_PORT));
  }
}
