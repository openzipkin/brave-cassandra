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
package cassandra;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.Server;
import org.junit.rules.ExternalResource;

/** This is a simplified version of code that exists in org.apache.cassandra.cql3.CQLTester */
public class CassandraRule extends ExternalResource {
  private static Server server;
  private static final int nativePort;
  private static final InetAddress nativeAddr;

  public InetSocketAddress contactPoint() {
    return new InetSocketAddress(nativeAddr.getHostAddress(), nativePort);
  }

  static {
    System.setProperty("cassandra.config", "test-cassandra.yaml");
    DatabaseDescriptor.daemonInitialization();
    nativeAddr = InetAddress.getLoopbackAddress();

    try {
      try (ServerSocket serverSocket = new ServerSocket(0)) {
        nativePort = serverSocket.getLocalPort();
      }
      Thread.sleep(250);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void cleanupAndLeaveDirs() throws IOException {
    CommitLog.instance.stopUnsafe(true);
    DatabaseDescriptor.createAllDirectories();
    cleanup();
    DatabaseDescriptor.createAllDirectories();
    CommitLog.instance.restartUnsafe();
  }

  private static void cleanup() {
    // clean up commitlog
    String[] directoryNames = {
      DatabaseDescriptor.getCommitLogLocation(),
    };
    for (String dirName : directoryNames) {
      File dir = new File(dirName);
      if (!dir.exists()) {
        throw new RuntimeException("No such directory: " + dir.getAbsolutePath());
      }
      FileUtils.deleteRecursive(dir);
    }

    File cdcDir = new File(DatabaseDescriptor.getCDCLogLocation());
    if (cdcDir.exists()) {
      FileUtils.deleteRecursive(cdcDir);
    }

    cleanupSavedCaches();

    // clean up data directory which are stored as data directory/keyspace/data files
    for (String dirName : DatabaseDescriptor.getAllDataFileLocations()) {
      File dir = new File(dirName);
      if (!dir.exists()) {
        throw new RuntimeException("No such directory: " + dir.getAbsolutePath());
      }
      FileUtils.deleteRecursive(dir);
    }
  }

  private static void cleanupSavedCaches() {
    File cachesDir = new File(DatabaseDescriptor.getSavedCachesLocation());
    if (!cachesDir.exists() || !cachesDir.isDirectory()) return;

    FileUtils.delete(cachesDir.listFiles());
  }

  @Override
  protected void before() {
    if (server != null) return;

    DatabaseDescriptor.daemonInitialization();

    // Cleanup first
    try {
      cleanupAndLeaveDirs();
    } catch (IOException e) {
      throw new RuntimeException("Failed to cleanup and recreate directories.", e);
    }

    Keyspace.setInitialized();
    SystemKeyspace.persistLocalMetadata();
    SystemKeyspace.finishStartup();
    StorageService.instance.initServer();

    server = new Server.Builder().withHost(nativeAddr).withPort(nativePort).build();
    server.start();
  }

  @Override
  protected void after() {
    if (server != null) server.stop();
  }
}
