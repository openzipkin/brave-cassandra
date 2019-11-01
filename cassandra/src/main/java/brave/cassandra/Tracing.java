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

import brave.Span;
import brave.SpanCustomizer;
import brave.Tracer;
import brave.internal.Nullable;
import brave.propagation.B3SingleFormat;
import brave.propagation.TraceContextOrSamplingFlags;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.utils.FBUtilities;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.urlconnection.URLConnectionSender;

import static brave.Span.Kind.SERVER;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * This creates Zipkin server spans for incoming cassandra requests. Spans are created when there's
 * a tracing component available, and the incoming payload is not explicitly unsampled.
 *
 * <h3>Configuring a tracing component</h3>
 *
 * If the system property "zipkin.http_endpoint" is set, a basic tracing component is setup.
 *
 * <p>Otherwise, {@link brave.Tracing#current()} is used. This relies on external bootstrapping of
 * {@link brave.Tracing}.
 *
 * <p>Alternatively, you can subclass this and fix configuration to your favorite mechanism.
 */
public class Tracing extends org.apache.cassandra.tracing.Tracing {
  final InetAddress coordinator = FBUtilities.getLocalAddress();
  final TracingComponent component;

  public Tracing(brave.Tracing tracing) { // subclassable to pin configuration
    component = new TracingComponent.Explicit(tracing);
  }

  public Tracing() {
    String endpoint = System.getProperty("zipkin.http_endpoint");
    if (endpoint == null) {
      component = new TracingComponent.Current();
      return;
    }
    AsyncReporter<zipkin2.Span> spanReporter =
        AsyncReporter.builder(URLConnectionSender.create(endpoint))
            .build(endpoint.contains("v2") ? SpanBytesEncoder.JSON_V2 : SpanBytesEncoder.JSON_V1);
    brave.Tracing tracing =
        brave.Tracing.newBuilder()
            .localServiceName(System.getProperty("zipkin.service_name", "cassandra"))
            .spanReporter(spanReporter)
            .build();
    component = new TracingComponent.Explicit(tracing);
  }

  /**
   * When tracing is enabled and available, this tries to extract trace keys from the custom
   * payload. If that's possible, it re-uses the trace identifiers and starts a server span.
   * Otherwise, a new trace is created.
   */
  @Override protected final UUID newSession(
      UUID sessionId, TraceType traceType, Map<String, ByteBuffer> customPayload) {
    Tracer tracer = component.tracer();
    if (tracer == null || traceType == TraceType.NONE) {
      return super.newSession(sessionId, traceType, customPayload);
    }
    Span span = spanFromPayload(tracer, customPayload).kind(SERVER);

    // override instead of call from super as otherwise we cannot store a reference to the span
    assert get() == null;
    TraceState state = new ZipkinTraceState(coordinator, sessionId, traceType, span);
    set(state);
    sessions.put(sessionId, state);
    return sessionId;
  }

  /** This extracts the RPC span encoded in the custom payload, or starts a new trace */
  Span spanFromPayload(Tracer tracer, @Nullable Map<String, ByteBuffer> payload) {
    ByteBuffer b3 = payload != null ? payload.get("b3") : null;
    if (b3 == null) return tracer.nextSpan();
    TraceContextOrSamplingFlags extracted = B3SingleFormat.parseB3SingleFormat(UTF_8.decode(b3));
    if (extracted == null) return tracer.nextSpan();
    return tracer.nextSpan(extracted);
  }

  @Override
  protected final void stopSessionImpl() {
    ZipkinTraceState state = (ZipkinTraceState) get();
    if (state != null) state.incoming.finish();
  }

  @Override
  public final TraceState begin(
      String request, InetAddress client, Map<String, String> parameters) {
    ZipkinTraceState state = ((ZipkinTraceState) get());
    Span span = state.incoming;
    if (span.isNoop()) return state;

    // request name example: "Execute CQL3 prepared query"
    parseRequest(state, request, parameters, span);
    // observed parameter keys include page_size, consistency_level, serial_consistency_level, query

    span.remoteIpAndPort(client.getHostAddress(), 0);
    span.start();
    return state;
  }

  /** Defaults to the trace type. Override to use the request name as the span name */
  protected String parseSpanName(TraceState state, String request) {
    return state.traceType.name();
  }

  /**
   * Override to change what data from the statement are parsed into the span representing it. By
   * default, this sets the span name to trace type and tags {@link
   * CassandraTraceKeys#CASSANDRA_REQUEST} and the {@link CassandraTraceKeys#CASSANDRA_SESSION_ID}.
   *
   * <p>If you only want to change the span name, you can override {@link #parseSpanName(TraceState,
   * String)} instead.
   *
   * @see #parseSpanName(TraceState, String)
   */
  protected void parseRequest(
      TraceState state, String request, Map<String, String> parameters, SpanCustomizer customizer) {
    customizer.name(parseSpanName(state, request));
    customizer.tag(CassandraTraceKeys.CASSANDRA_REQUEST, request);
    customizer.tag(CassandraTraceKeys.CASSANDRA_SESSION_ID, state.sessionId.toString());
  }

  @Override
  protected final TraceState newTraceState(
      InetAddress coordinator, UUID sessionId, TraceType traceType) {
    throw new AssertionError();
  }

  @Override
  public final void trace(ByteBuffer sessionId, String message, int ttl) {
    // not current tracing outbound messages
  }

  static final class ZipkinTraceState extends TraceState {
    final Span incoming;

    ZipkinTraceState(InetAddress coordinator, UUID sessionId, TraceType traceType, Span incoming) {
      super(coordinator, sessionId, traceType);
      this.incoming = incoming;
    }

    @Override
    protected void traceImpl(String message) {
      incoming.annotate(message); // skip creating local spans for now
    }
  }
}
