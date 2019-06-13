/*
 * Copyright 2017-2019 The OpenZipkin Authors
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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.After;
import org.junit.Test;
import zipkin2.Span;

import static org.assertj.core.api.Assertions.assertThat;

public class TracingTest {
  List<Span> spans = new ArrayList<>();
  brave.Tracing tracing = brave.Tracing.newBuilder()
      .spanReporter(spans::add)
      .build();
  Tracing cassandraTracing = new Tracing(tracing);

  @After public void tearDown() {
    tracing.close();
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
        .extracting(b -> b.isNoop())
        .isEqualTo(Boolean.TRUE);
  }
}
