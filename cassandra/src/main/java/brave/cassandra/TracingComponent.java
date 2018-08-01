/*
 * Copyright 2017-2018 The OpenZipkin Authors
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
import brave.propagation.Propagation.Getter;
import brave.propagation.TraceContext;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Map;

abstract class TracingComponent {
  static final Charset UTF_8 = Charset.forName("UTF-8");

  /** Getter that pulls trace fields from ascii values */
  static final Getter<Map<String, ByteBuffer>, String> GETTER =
      new Getter<Map<String, ByteBuffer>, String>() {
        @Override public String get(Map<String, ByteBuffer> carrier, String key) {
          ByteBuffer buf = carrier.get(key);
          return buf != null ? UTF_8.decode(buf).toString() : null;
        }

        @Override public String toString() {
          return "Map::get";
        }
      };

  abstract Tracer tracer();

  abstract TraceContext.Extractor<Map<String, ByteBuffer>> extractor();

  static final class Current extends TracingComponent {
    @Override
    Tracer tracer() {
      return brave.Tracing.currentTracer();
    }

    @Override
    TraceContext.Extractor<Map<String, ByteBuffer>> extractor() {
      brave.Tracing tracing = brave.Tracing.current();
      return tracing != null ? tracing.propagation().extractor(GETTER) : null;
    }
  }

  static final class Explicit extends TracingComponent {
    final Tracer tracer;
    final TraceContext.Extractor<Map<String, ByteBuffer>> extractor;

    Explicit(brave.Tracing tracing) {
      if (tracing == null) throw new NullPointerException("tracing == null");
      this.tracer = tracing.tracer();
      this.extractor = tracing.propagation().extractor(GETTER);
    }

    @Override
    Tracer tracer() {
      return tracer;
    }

    @Override
    TraceContext.Extractor<Map<String, ByteBuffer>> extractor() {
      return extractor;
    }
  }
}
