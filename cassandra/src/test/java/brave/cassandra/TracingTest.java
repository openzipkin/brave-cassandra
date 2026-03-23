/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package brave.cassandra;

import brave.propagation.StrictCurrentTraceContext;
import brave.test.TestSpanHandler;
import java.nio.ByteBuffer;
import java.util.Collections;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TracingTest {
  StrictCurrentTraceContext currentTraceContext = StrictCurrentTraceContext.create();
  TestSpanHandler spans = new TestSpanHandler();
  brave.Tracing tracing = brave.Tracing.newBuilder()
      .currentTraceContext(currentTraceContext).addSpanHandler(spans).build();
  Tracing cassandraTracing = new Tracing(tracing);

  @AfterEach public void tearDown() {
    tracing.close();
    currentTraceContext.close();
  }

  @Test void spanFromPayload_startsTraceOnNullPayload() {
    assertThat(cassandraTracing.spanFromPayload(tracing.tracer(), null))
        .isNotNull();
  }

  @Test void spanFromPayload_startsTraceOnAbsentB3SingleEntry() {
    assertThat(cassandraTracing.spanFromPayload(tracing.tracer(), Collections.emptyMap()))
        .isNotNull();
  }

  @Test void spanFromPayload_resumesTraceOnB3SingleEntry() {
    assertThat(cassandraTracing.spanFromPayload(tracing.tracer(), Collections.singletonMap("b3",
        ByteBuffer.wrap(new byte[] {'0'}))))
        .extracting(brave.Span::isNoop)
        .isEqualTo(Boolean.TRUE);
  }
}
