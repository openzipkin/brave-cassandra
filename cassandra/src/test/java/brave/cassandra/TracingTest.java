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

import brave.propagation.StrictCurrentTraceContext;
import brave.test.TestSpanHandler;
import java.nio.ByteBuffer;
import java.util.Collections;
import org.junit.After;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TracingTest {
  StrictCurrentTraceContext currentTraceContext = StrictCurrentTraceContext.create();
  TestSpanHandler spans = new TestSpanHandler();
  brave.Tracing tracing = brave.Tracing.newBuilder()
      .currentTraceContext(currentTraceContext).addSpanHandler(spans).build();
  Tracing cassandraTracing = new Tracing(tracing);

  @After public void tearDown() {
    tracing.close();
    currentTraceContext.close();
  }

  @Test public void spanFromPayload_startsTraceOnNullPayload() {
    assertThat(cassandraTracing.spanFromPayload(tracing.tracer(), null))
        .isNotNull();
  }

  @Test public void spanFromPayload_startsTraceOnAbsentB3SingleEntry() {
    assertThat(cassandraTracing.spanFromPayload(tracing.tracer(), Collections.emptyMap()))
        .isNotNull();
  }

  @Test public void spanFromPayload_resumesTraceOnB3SingleEntry() {
    assertThat(cassandraTracing.spanFromPayload(tracing.tracer(), Collections.singletonMap("b3",
        ByteBuffer.wrap(new byte[] {'0'}))))
        .extracting(brave.Span::isNoop)
        .isEqualTo(Boolean.TRUE);
  }
}
