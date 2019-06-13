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
package brave.cassandra.driver;

import brave.Span;
import brave.Tracer;
import brave.Tracing;
import brave.propagation.B3SingleFormat;
import brave.sampler.Sampler;
import com.datastax.driver.core.AbstractSession;
import com.datastax.driver.core.CloseFuture;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

import static brave.Span.Kind.CLIENT;

public class TracingSession extends AbstractSession {
  public static Session create(Tracing tracing, Session delegate) {
    return new TracingSession(CassandraClientTracing.create(tracing), delegate);
  }

  public static Session create(CassandraClientTracing cassandraTracing, Session delegate) {
    ProtocolVersion version =
        delegate.getCluster().getConfiguration().getProtocolOptions().getProtocolVersion();
    if (version.compareTo(ProtocolVersion.V4) >= 0 && cassandraTracing.propagationEnabled()) {
      return new PropagatingTracingSession(cassandraTracing, delegate);
    }
    return new TracingSession(cassandraTracing, delegate);
  }

  final Tracer tracer;
  final CassandraClientSampler sampler;
  final CassandraClientParser parser;
  final String remoteServiceName;
  final Session delegate;

  TracingSession(CassandraClientTracing cassandraTracing, Session target) {
    if (cassandraTracing == null) throw new NullPointerException("cassandraTracing == null");
    if (target == null) throw new NullPointerException("target == null");
    this.delegate = target;
    tracer = cassandraTracing.tracing().tracer();
    sampler = cassandraTracing.sampler();
    parser = cassandraTracing.parser();
    String remoteServiceName = cassandraTracing.remoteServiceName();
    this.remoteServiceName =
        remoteServiceName != null ? remoteServiceName : target.getCluster().getClusterName();
  }

  @Override
  public ResultSetFuture executeAsync(Statement statement) {
    Span span = nextSpan(statement);
    if (!span.isNoop()) parser.request(statement, span.kind(CLIENT));

    maybeDecorate(statement, span);

    span.start();
    ResultSetFuture result;
    try {
      result = delegate.executeAsync(statement);
    } catch (RuntimeException | Error e) {
      if (span.isNoop()) throw e;
      span.error(e);
      span.finish();
      throw e;
    }
    if (span.isNoop()) return result; // don't add callback on noop
    Futures.addCallback(
        result,
        new FutureCallback<ResultSet>() {
          @Override
          public void onSuccess(ResultSet result) {
            InetSocketAddress host = result.getExecutionInfo().getQueriedHost().getSocketAddress();
            span.remoteIpAndPort(host.getHostString(), host.getPort());
            span.remoteServiceName(remoteServiceName);
            parser.response(result, span);
            span.finish();
          }

          @Override
          public void onFailure(Throwable e) {
            span.error(e);
            span.finish();
          }
        });
    return result;
  }

  void maybeDecorate(Statement statement, Span span) {
  }

  /** Creates a potentially noop span representing this request */
  Span nextSpan(Statement statement) {
    if (tracer.currentSpan() != null) return tracer.nextSpan();

    // If there was no parent, we are making a new trace. Try to sample the request.
    Boolean sampled = sampler.trySample(statement);
    if (sampled == null) return tracer.newTrace(); // defer sampling decision to trace ID
    return tracer.withSampler(sampled ? Sampler.ALWAYS_SAMPLE : Sampler.NEVER_SAMPLE).nextSpan();
  }

  @Override
  protected ListenableFuture<PreparedStatement> prepareAsync(
      String query, Map<String, ByteBuffer> customPayload) {
    SimpleStatement statement = new SimpleStatement(query);
    statement.setOutgoingPayload(customPayload);
    return prepareAsync(statement);
  }

  @Override
  public ListenableFuture<PreparedStatement> prepareAsync(String query) {
    return delegate.prepareAsync(query);
  }

  @Override
  public String getLoggedKeyspace() {
    return delegate.getLoggedKeyspace();
  }

  @Override
  public Session init() {
    return delegate.init();
  }

  @Override
  public ListenableFuture<Session> initAsync() {
    return delegate.initAsync();
  }

  @Override
  public ListenableFuture<PreparedStatement> prepareAsync(RegularStatement statement) {
    return delegate.prepareAsync(statement);
  }

  @Override
  public CloseFuture closeAsync() {
    return delegate.closeAsync();
  }

  @Override
  public boolean isClosed() {
    return delegate.isClosed();
  }

  @Override
  public Cluster getCluster() {
    return delegate.getCluster();
  }

  @Override
  public State getState() {
    return delegate.getState();
  }

  // o.a.c.tracing.Tracing.newSession must use the same propagation format
  static final class PropagatingTracingSession extends TracingSession {

    PropagatingTracingSession(CassandraClientTracing cassandraTracing, Session target) {
      super(cassandraTracing, target);
    }

    @Override
    void maybeDecorate(Statement statement, Span span) {
      statement.enableTracing();
      Map<String, ByteBuffer> payload = new LinkedHashMap<>();
      if (statement.getOutgoingPayload() != null) {
        payload.putAll(statement.getOutgoingPayload());
      }
      payload.put("b3", ByteBuffer.wrap(B3SingleFormat.writeB3SingleFormatAsBytes(span.context())));
      statement.setOutgoingPayload(payload);
    }
  }
}
