/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package cassandra;

import brave.handler.MutableSpan;
import brave.handler.SpanHandler;
import brave.propagation.TraceContext;

// TODO: add to brave as it will be able to ensure all methods are dispatched
public abstract class ForwardingSpanHandler extends SpanHandler {
  protected abstract SpanHandler delegate();

  @Override public boolean begin(TraceContext context, MutableSpan span, TraceContext parent) {
    return delegate().begin(context, span, parent);
  }

  @Override public boolean end(TraceContext context, MutableSpan span, Cause cause) {
    return delegate().end(context, span, cause);
  }

  @Override public boolean handlesAbandoned() {
    return delegate().handlesAbandoned();
  }

  @Override public String toString() {
    return delegate().toString();
  }
}
