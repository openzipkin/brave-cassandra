[![Gitter chat](http://img.shields.io/badge/gitter-join%20chat%20%E2%86%92-brightgreen.svg)](https://gitter.im/openzipkin/zipkin)
[![Build Status](https://travis-ci.com/openzipkin/brave-cassandra.svg?branch=master)](https://travis-ci.com/openzipkin/brave-cassandra)
[![Maven Central](https://img.shields.io/maven-central/v/io.zipkin.brave.cassandra/brave-instrumentation-cassandra.svg)](https://search.maven.org/search?q=g:io.brave.cassandra%20AND%20a:brave-instrumentation-cassandra)

# brave-cassandra
Brave for Apache Cassandra allows you to trace activities started from the Datastax Java Driver all the way into Cassandra.

This repository includes tracing wrappers for [DataStax Java Driver](https://github.com/datastax/java-driver) and an [Apache Cassandra tracing implementation](https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/tracing/Tracing.java)

`brave.cassandra.Tracing` extracts trace state from the custom payload
of incoming requests. How long each request takes, each suboperation,
and relevant tags like the session ID are reported to Zipkin.
    
`brave.cassandra.driver.TracingSession` tracks the client-side of cassandra and
adds trace context to the custom payload of outgoing requests. If
server integration is in place, cassandra will contribute data to these
RPC spans.

## Artifacts
All artifacts publish to the group ID "io.zipkin.brave.cassandra". We use a common
release version for all components.

### Library Releases
Snapshots are uploaded to [Sonatype](https://oss.sonatype.org/content/repositories/releases/io/zipkin/brave/cassandra) which
synchronizes with [Maven Central](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22io.zipkin.brave.cassandra%22)

### Library Snapshots
Snapshots are uploaded to [Sonatype](https://oss.sonatype.org/content/repositories/snapshots) after
commits to master.
