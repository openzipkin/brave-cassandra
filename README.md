[![Gitter chat](http://img.shields.io/badge/gitter-join%20chat%20%E2%86%92-brightgreen.svg)](https://gitter.im/openzipkin/zipkin)
[![Build Status](https://img.shields.io/jenkins/s/https/builds.apache.org/job/incubator-zipkin-brave-cassandra.svg)](https://builds.apache.org/blue/organizations/jenkins/incubator-zipkin-brave-cassandra)
[![Maven Central](https://img.shields.io/maven-central/v/org.apache.zipkin.brave.cassandra/brave-instrumentation-cassandra.svg)](https://search.maven.org/search?q=g:org.apache.zipkin.brave.cassandra%20AND%20a:brave-instrumentation-cassandra)

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
Artifacts are under the maven group id `org.apache.zipkin.brave.cassandra`
### Source Releases
Source Releases are uploaded to [Apache](https://dist.apache.org/repos/dist/release/incubator/zipkin/brave-cassandra)
### Binary Releases
Binary Releases are uploaded to [Apache](https://repository.apache.org/service/local/staging/deploy/maven2) and synchronized to [Maven Central](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.apache.zipkin.brave.cassandra%22)
### Binary Snapshots
Binary Snapshots are uploaded to [Apache](https://repository.apache.org/content/repositories/snapshots/) after commits to master.
