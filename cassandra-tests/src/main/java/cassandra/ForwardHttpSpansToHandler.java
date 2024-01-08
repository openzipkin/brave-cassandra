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

import brave.handler.MutableSpan;
import brave.handler.SpanHandler;
import brave.test.ITRemote;
import java.io.IOException;
import java.util.Map;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okio.Buffer;
import okio.GzipSource;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import zipkin2.Annotation;
import zipkin2.Span;
import zipkin2.codec.SpanBytesDecoder;

/**
 * Accepts Zipkin JSON or protobuf messages to {@code POST http://localhost:${port}/api/v2/spans}
 * and replays them through a {@link SpanHandler}. This allows re-use of {@link ITRemote} when using
 * instrumentation that do not expose {@link SpanHandler} or run in Docker.
 */
// Temporary until moved into brave-tests.
public class ForwardHttpSpansToHandler implements BeforeAllCallback, AfterAllCallback {
  final MockWebServer zipkin = new MockWebServer();

  /**
   * The port variable in {@code POST http://localhost:${port}/api/v2/spans}.
   *
   * <p><h3>Testcontainers note</h3>
   * This is typically used to allow instrumentation in Docker containers to be tested the same as
   * in- memory. There's a catch to using this in Testcontainers 1.15, which is you have to clear
   * the port prior to starting the container.
   *
   * <pre>${code
   *  // Allows http://host.testcontainers.internal:" + zipkin.httpPort() + "/api/v2/spans"
   *  Testcontainers.exposeHostPorts(zipkin.httpPort());
   * }</pre>
   */
  public int httpPort() {
    return zipkin.getPort();
  }

  public ForwardHttpSpansToHandler(SpanHandler spanHandler) {
    if (spanHandler == null) throw new NullPointerException("spanHandler == null");
    zipkin.setDispatcher(new ZipkinDispatcher(spanHandler));
  }

  @Override public void beforeAll(ExtensionContext extensionContext) throws Exception {
    zipkin.start();
  }

  @Override public void afterAll(ExtensionContext extensionContext) throws Exception {
    zipkin.shutdown();
  }

  static final class ZipkinDispatcher extends Dispatcher {
    final SpanHandler spanHandler;

    ZipkinDispatcher(SpanHandler spanHandler) {
      this.spanHandler = spanHandler;
    }

    @Override public MockResponse dispatch(RecordedRequest request) {
      if ("POST".equals(request.getMethod())) {
        String type = request.getHeader("Content-Type");
        if ("/api/v2/spans".equals(request.getPath())) {
          SpanBytesDecoder decoder = type != null && type.contains("/x-protobuf")
              ? SpanBytesDecoder.PROTO3
              : SpanBytesDecoder.JSON_V2;
          return acceptSpans(request, decoder);
        }
      } else { // unsupported method
        return new MockResponse().setResponseCode(405);
      }
      return new MockResponse().setResponseCode(404);
    }

    MockResponse acceptSpans(RecordedRequest request, SpanBytesDecoder decoder) {
      byte[] body = request.getBody().readByteArray();
      String encoding = request.getHeader("Content-Encoding");
      if (encoding != null && encoding.contains("gzip")) {
        try {
          Buffer result = new Buffer();
          GzipSource source = new GzipSource(new Buffer().write(body));
          while (source.read(result, Integer.MAX_VALUE) != -1) ;
          body = result.readByteArray();
        } catch (IOException e) {
          return new MockResponse().setResponseCode(400).setBody("Cannot gunzip spans");
        }
      }

      if (body.length == 0) return new MockResponse().setResponseCode(202); // lenient on empty

      for (Span span : decoder.decodeList(body)) {
        spanHandler.end(null, toMutableSpan(span), SpanHandler.Cause.FINISHED);
      }

      return new MockResponse();
    }
  }

  static MutableSpan toMutableSpan(Span span) {
    MutableSpan result = new MutableSpan();
    result.traceId(span.traceId());
    result.parentId(span.parentId());
    result.id(span.id());
    result.name(span.name());
    result.startTimestamp(span.timestampAsLong());
    if (span.timestampAsLong() != 0L && span.durationAsLong() != 0L) {
      result.finishTimestamp(span.timestampAsLong() + span.durationAsLong());
    }
    if (span.kind() != null) {
      result.kind(brave.Span.Kind.valueOf(span.kind().name()));
    }
    if (span.localEndpoint() != null) {
      result.localServiceName(span.localEndpoint().serviceName());
      String ipv6 = span.localEndpoint().ipv6();
      if (ipv6 != null) {
        result.localIp(ipv6);
      } else {
        result.localIp(span.localEndpoint().ipv4());
      }
      result.localPort(span.localEndpoint().portAsInt());
    }
    if (span.remoteEndpoint() != null) {
      result.remoteServiceName(span.remoteEndpoint().serviceName());
      String ipv6 = span.remoteEndpoint().ipv6();
      if (ipv6 != null) {
        result.remoteIp(ipv6);
      } else {
        result.remoteIp(span.remoteEndpoint().ipv4());
      }
      result.remotePort(span.remoteEndpoint().portAsInt());
    }
    for (Annotation annotation : span.annotations()) {
      result.annotate(annotation.timestamp(), annotation.value());
    }
    for (Map.Entry<String, String> tag : span.tags().entrySet()) {
      result.tag(tag.getKey(), tag.getValue());
    }
    if (Boolean.TRUE.equals(span.shared())) result.setShared();
    if (Boolean.TRUE.equals(span.debug())) result.setDebug();
    return result;
  }
}
