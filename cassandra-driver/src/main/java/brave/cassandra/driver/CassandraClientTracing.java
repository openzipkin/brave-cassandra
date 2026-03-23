/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package brave.cassandra.driver;

import brave.Tracing;
import brave.internal.Nullable;
import brave.propagation.Propagation;

public final class CassandraClientTracing {
  public static CassandraClientTracing create(Tracing tracing) {
    return newBuilder(tracing).build();
  }

  public static Builder newBuilder(Tracing tracing) {
    if (tracing == null) throw new NullPointerException("tracing == null");
    return new Builder(tracing);
  }

  public static final class Builder {
    final Tracing tracing;
    CassandraClientParser parser = new CassandraClientParser();
    CassandraClientSampler sampler = CassandraClientSampler.TRACE_ID;
    boolean propagationEnabled = false;
    String remoteServiceName;

    Builder(Tracing tracing) {
      this.tracing = tracing;
    }

    Builder(CassandraClientTracing source) {
      this.tracing = source.tracing;
      this.parser = source.parser;
      this.sampler = source.sampler;
      this.propagationEnabled = source.propagationEnabled;
      this.remoteServiceName = source.remoteServiceName;
    }

    /** @see CassandraClientTracing#parser() */
    public Builder parser(CassandraClientParser parser) {
      if (parser == null) throw new NullPointerException("parser == null");
      this.parser = parser;
      return this;
    }

    /** @see CassandraClientTracing#sampler() */
    public Builder sampler(CassandraClientSampler sampler) {
      if (sampler == null) throw new NullPointerException("sampler == null");
      this.sampler = sampler;
      return this;
    }

    /** @see CassandraClientTracing#propagationEnabled() */
    public Builder propagationEnabled(boolean propagationEnabled) {
      this.propagationEnabled = propagationEnabled;
      return this;
    }

    public Builder remoteServiceName(@Nullable String remoteServiceName) {
      this.remoteServiceName = remoteServiceName;
      return this;
    }

    public CassandraClientTracing build() {
      return new CassandraClientTracing(this);
    }
  }

  final Tracing tracing;
  final CassandraClientParser parser;
  final CassandraClientSampler sampler;
  final boolean propagationEnabled;
  @Nullable final String remoteServiceName;

  public Builder toBuilder() {
    return new Builder(this);
  }

  CassandraClientTracing(Builder builder) {
    this.tracing = builder.tracing;
    this.parser = builder.parser;
    this.sampler = builder.sampler;
    this.propagationEnabled = builder.propagationEnabled;
    this.remoteServiceName = builder.remoteServiceName;
  }

  public Tracing tracing() {
    return tracing;
  }

  public CassandraClientParser parser() {
    return parser;
  }

  /**
   * Used by cassandra clients to indicate the name of the destination service. Defaults to the
   * cluster name.
   *
   * <p>As this is endpoint-specific, it is typical to create a scoped instance of {@linkplain
   * CassandraClientTracing} to assign this value.
   *
   * <p>For example:
   *
   * <pre>{@code
   * production = TracingSession.create(httpTracing.remoteServiceName("production"));
   * }</pre>
   *
   * @see brave.Span#remoteServiceName(String)
   */
  @Nullable public String remoteServiceName() {
    return remoteServiceName;
  }

  /**
   * Scopes this component for a client of the indicated server.
   *
   * @see #remoteServiceName()
   */
  public CassandraClientTracing clientOf(String remoteServiceName) {
    return toBuilder().remoteServiceName(remoteServiceName).build();
  }

  /**
   * When true, trace contexts will be propagated downstream based on the {@link
   * Tracing.Builder#propagationFactory(Propagation.Factory) configured implementation}.
   *
   * <p>Warning: sometimes this can cause connection failures. As such, consider this feature
   * experimental.
   */
  public boolean propagationEnabled() {
    return propagationEnabled;
  }

  /**
   * Returns an overriding sampling decision for a new trace. Defaults to ignore the request and use
   * the {@link CassandraClientSampler#TRACE_ID trace ID instead}.
   */
  public CassandraClientSampler sampler() {
    return sampler;
  }
}
