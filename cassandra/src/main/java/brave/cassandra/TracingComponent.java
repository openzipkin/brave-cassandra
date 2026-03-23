/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package brave.cassandra;

import brave.Tracer;

abstract class TracingComponent {

  abstract Tracer tracer();

  static final class Current extends TracingComponent {
    @Override Tracer tracer() {
      return brave.Tracing.currentTracer();
    }
  }

  static final class Explicit extends TracingComponent {
    final Tracer tracer;

    Explicit(brave.Tracing tracing) {
      if (tracing == null) throw new NullPointerException("tracing == null");
      this.tracer = tracing.tracer();
    }

    @Override Tracer tracer() {
      return tracer;
    }
  }
}
