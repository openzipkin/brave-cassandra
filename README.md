[![Gitter chat](http://img.shields.io/badge/gitter-join%20chat%20%E2%86%92-brightgreen.svg)](https://gitter.im/openzipkin/zipkin)
[![Build Status](https://circleci.com/gh/openzipkin/brave-cassandra.svg?style=svg)](https://circleci.com/gh/openzipkin/brave-cassandra)
[![Download](https://api.bintray.com/packages/openzipkin/maven/brave-cassandra/images/download.svg)](https://bintray.com/openzipkin/maven/brave-cassandra/_latestVersion)

# brave-cassandra
This contains tracing instrumentation for [Cassandra](https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/tracing/Tracing.java) and the [DataStax Java Driver](https://github.com/datastax/java-driver).
    
`brave.cassandra.Tracing` extracts trace state from the custom payload
of incoming requests. How long each request takes, each suboperation,
and relevant tags like the session ID are reported to Zipkin.
    
`brave.cassandra.driver.TracingSession` tracks the client-side of cassandra and
adds trace context to the custom payload of outgoing requests. If
server integration is in place, cassandra will contribute data to these
RPC spans.

## Artifacts
Artifacts are under the maven group id `io.zipkin.brave.cassandra`
### Library Releases
Releases are uploaded to [Bintray](https://bintray.com/openzipkin/maven/brave-cassandra) and synchronized to [Maven Central](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22io.zipkin.brave.cassandra%22)
### Library Snapshots
Snapshots are uploaded to [JFrog](http://oss.jfrog.org/artifactory/oss-snapshot-local) after commits to master.
