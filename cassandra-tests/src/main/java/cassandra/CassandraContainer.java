/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package cassandra;

import java.net.InetSocketAddress;
import java.time.Duration;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import static org.testcontainers.utility.DockerImageName.parse;

public class CassandraContainer extends GenericContainer<CassandraContainer> {
  public CassandraContainer() {
    super(parse("ghcr.io/openzipkin/zipkin-cassandra:3.0.2"));
    waitStrategy = Wait.forHealthcheck();
    addExposedPort(9042);
    withStartupTimeout(Duration.ofMinutes(2));
    withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(CassandraContainer.class)));
  }

  public InetSocketAddress contactPoint() {
    return new InetSocketAddress(getHost(), getMappedPort(9042));
  }
}
