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
package cassandra;

import java.net.InetSocketAddress;
import java.time.Duration;
import org.junit.AssumptionViolatedException;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import static org.testcontainers.utility.DockerImageName.parse;

public class CassandraContainer extends GenericContainer<CassandraContainer> {
  public CassandraContainer() {
    super(parse("ghcr.io/openzipkin/zipkin-cassandra:2.27.0"));
    waitStrategy = Wait.forHealthcheck();
    addExposedPort(9042);
    withStartupTimeout(Duration.ofMinutes(2));
    withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(CassandraContainer.class)));
  }

  public InetSocketAddress contactPoint() {
    return new InetSocketAddress(getHost(), getMappedPort(9042));
  }
}
