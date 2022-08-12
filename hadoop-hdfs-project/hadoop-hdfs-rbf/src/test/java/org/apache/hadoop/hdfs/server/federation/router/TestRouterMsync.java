/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.NamenodeContext;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.AlignmentContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_STATE_CONTEXT_ENABLED_KEY;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.NAMENODES;

public class TestRouterMsync {

  private static final Logger LOG = LoggerFactory.getLogger(TestRouterMsync.class);

  private static MiniRouterDFSCluster cluster;

  public static void createCluster(boolean enableNewMsyncInRBF, boolean enableNewMsyncInNN)
      throws IOException {
    try {
      Configuration conf = new Configuration();
      conf.setBoolean(RBFConfigKeys.DFS_ROUTER_OBSERVER_READ_ENABLE, true);
      conf.setBoolean(DFS_NAMENODE_STATE_CONTEXT_ENABLED_KEY, true);
      conf.setInt(RBFConfigKeys.DFS_ROUTER_OBSERVER_AUTO_MSYNC_PERIOD, 60000);
      conf.setBoolean(DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY, true);
      conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 0);
      if (enableNewMsyncInNN) {
        conf.set(DFSConfigKeys.DFS_NAMENODE_MSYNC_RPC_ADDRESS_KEY, "0.0.0.0:0");
      }

      cluster = new MiniRouterDFSCluster(true, 1, 3);
      cluster.addNamenodeOverrides(conf);

      // Start NNs and DNs and wait until ready
      cluster.startCluster(true);

      if (enableNewMsyncInRBF) {
        conf.setBoolean(RBFConfigKeys.DSF_ROUTER_OBSERVER_ENABLE_NEW_MSYNC_SERVER_KEY, true);
        for (NamenodeContext namenodeContext : cluster.getNamenodes()) {
          String nsId = namenodeContext.getNameserviceId();
          String nnId = namenodeContext.getNamenodeId();

          String addrKey = DFSUtilClient.concatSuffixes(
              DFSConfigKeys.DFS_NAMENODE_MSYNC_RPC_ADDRESS_KEY, nsId, nnId);
          InetSocketAddress msyncRpcAddr = namenodeContext.getNamenode().getNameNodeMSyncAddress();
          String value;
          if (msyncRpcAddr != null) {
            value = msyncRpcAddr.getAddress().getHostAddress() + ":" + msyncRpcAddr.getPort();
          } else {
            value = "0.0.0.0:" + ThreadLocalRandom.current().nextInt(10);
          }
          LOG.info("Will set {}={} into router conf.", addrKey, value);
          conf.set(addrKey, value);
        }
      }

      cluster.addRouterOverrides(conf);

      // Start routers with only an RPC service
      cluster.startRouters();

      // Register and verify all NNs with all routers
      cluster.registerNamenodes();
      cluster.waitNamenodeRegistration();
      // Setup the mount table
      cluster.installMockLocations();
      // Making one Namenodes active per nameservice
      if (cluster.isHighAvailability()) {
        for (String ns : cluster.getNameservices()) {
          cluster.switchToActive(ns, NAMENODES[0]);
          cluster.switchToStandby(ns, NAMENODES[1]);
          cluster.switchToObserver(ns, NAMENODES[2]);
        }
      }
      cluster.waitActiveNamespaces();
      cluster.waitObserverNamespaces(1);
    } catch (Exception e) {
      destroyCluster();
      throw new IOException("Cannot start federated cluster", e);
    }
  }

  @AfterClass
  public static void destroyCluster() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }


  @Test
  public void testMsynByNewServer() throws IOException {
    createCluster(true, true);
    RouterContext routerContext = cluster.getRandomRouter();
    FileSystem fileSystem = routerContext.getFileSystem();
    RouterRpcClient routerRpcClient = routerContext.getRouterRpcClient();
    ConnectionManager connectionManager = routerRpcClient.getConnectionManager();
    NameNode activeNameNode = cluster.getNamenodes().get(0).getNamenode();
    String nsId = cluster.getRandomNamenode().getNameserviceId();


    Path testPath =  new Path("/test_verify_new_msync_server");
    FSDataOutputStream outputStream = fileSystem.create(testPath);
    outputStream.write("hello world".getBytes());
    outputStream.close();

    long lastTxIdInNN = activeNameNode.getNamesystem().getFSImage().getLastAppliedOrWrittenTxId();

    // Observer Read and update the lastTxId in Router
    fileSystem.getFileStatus(testPath);

    AlignmentContext alignmentContext = connectionManager.getNsAlignmentContext(nsId);
    long lastTxIdInRouter = alignmentContext.getLastSeenStateId();

    Assert.assertEquals(lastTxIdInNN, lastTxIdInRouter);

    // Directly increase the txid of active namenode by setReplication RPC.
    boolean setReplication = activeNameNode.getRpcServer()
        .setReplication(testPath.toUri().getPath(), (short) 4);
    Assert.assertTrue(setReplication);

    final List<? extends FederationNamenodeContext> namenodes =
        routerRpcClient.getNamenodesForNameservice(nsId, false);

    // Msync by the new msync rpc server.
    boolean msycnWithNewServer = routerRpcClient.internalMsyncWithNewServer(
        UserGroupInformation.getCurrentUser(), namenodes);
    Assert.assertTrue(msycnWithNewServer);

    long lastTxIdInRouterWithNewServer = alignmentContext.getLastSeenStateId();
    lastTxIdInNN = activeNameNode.getNamesystem().getFSImage().getLastAppliedOrWrittenTxId();

    Assert.assertEquals(lastTxIdInNN, lastTxIdInRouterWithNewServer);
    Assert.assertEquals(lastTxIdInNN, lastTxIdInRouter + 1);
  }

  @Test
  public void testRBFEnableAndNNDisable() throws IOException {
    createCluster(true, false);
    RouterContext routerContext = cluster.getRandomRouter();
    FileSystem fileSystem = routerContext.getFileSystem();
    RouterRpcClient routerRpcClient = routerContext.getRouterRpcClient();
    ConnectionManager connectionManager = routerRpcClient.getConnectionManager();
    NameNode activeNameNode = cluster.getNamenodes().get(0).getNamenode();
    String nsId = cluster.getRandomNamenode().getNameserviceId();

    Path testPath =  new Path("/test_verify_new_msync_server");
    FSDataOutputStream outputStream = fileSystem.create(testPath);
    outputStream.write("hello world".getBytes());
    outputStream.close();

    long lastTxIdInNN = activeNameNode.getNamesystem().getFSImage().getLastAppliedOrWrittenTxId();

    // Observer Read and update the lastTxId in Router
    fileSystem.getFileStatus(testPath);

    AlignmentContext alignmentContext = connectionManager.getNsAlignmentContext(nsId);
    long lastTxIdInRouter = alignmentContext.getLastSeenStateId();
    Assert.assertEquals(lastTxIdInNN, lastTxIdInRouter);

    // Directly increase the txid of active namenode by setReplication RPC.
    boolean setReplication = activeNameNode.getRpcServer().setReplication(
        testPath.toUri().getPath(), (short) 4);
    Assert.assertTrue(setReplication);

    // Msync to new rpc server will return false.
    final List<? extends FederationNamenodeContext> namenodes =
        routerRpcClient.getNamenodesForNameservice(nsId, false);
    boolean msycnWithNewServer = routerRpcClient.internalMsyncWithNewServer(
        UserGroupInformation.getCurrentUser(), namenodes);
    Assert.assertFalse(msycnWithNewServer);

    long lastTxIdInRouterWithNewServer = alignmentContext.getLastSeenStateId();
    lastTxIdInNN = activeNameNode.getNamesystem().getFSImage().getLastAppliedOrWrittenTxId();
    Assert.assertNotEquals(lastTxIdInNN, lastTxIdInRouterWithNewServer);

    routerRpcClient.internalMsync(UserGroupInformation.getCurrentUser(), nsId);
    lastTxIdInRouterWithNewServer = alignmentContext.getLastSeenStateId();
    Assert.assertEquals(lastTxIdInNN, lastTxIdInRouterWithNewServer);
    Assert.assertEquals(lastTxIdInNN, lastTxIdInRouter + 1);
  }

  @Test
  public void testRBFDisableAndNNEnable() throws IOException {
    createCluster(false, true);
    RouterContext routerContext = cluster.getRandomRouter();
    FileSystem fileSystem = routerContext.getFileSystem();
    RouterRpcClient routerRpcClient = routerContext.getRouterRpcClient();
    ConnectionManager connectionManager = routerRpcClient.getConnectionManager();
    NameNode activeNameNode = cluster.getNamenodes().get(0).getNamenode();
    String nsId = cluster.getRandomNamenode().getNameserviceId();

    Path testPath =  new Path("/test_verify_new_msync_server");
    FSDataOutputStream outputStream = fileSystem.create(testPath);
    outputStream.write("hello world".getBytes());
    outputStream.close();

    long lastTxIdInNN = activeNameNode.getNamesystem().getFSImage().getLastAppliedOrWrittenTxId();
    fileSystem.getFileStatus(testPath);

    AlignmentContext alignmentContext = connectionManager.getNsAlignmentContext(nsId);
    long lastTxIdInRouter = alignmentContext.getLastSeenStateId();
    Assert.assertEquals(lastTxIdInNN, lastTxIdInRouter);

    boolean setReplication = activeNameNode.getRpcServer().setReplication(
        testPath.toUri().getPath(), (short) 4);
    Assert.assertTrue(setReplication);

    routerRpcClient.internalMsync(UserGroupInformation.getCurrentUser(), nsId);

    long lastTxIdInRouterWithNewServer = alignmentContext.getLastSeenStateId();
    lastTxIdInNN = activeNameNode.getNamesystem().getFSImage().getLastAppliedOrWrittenTxId();

    Assert.assertEquals(lastTxIdInNN, lastTxIdInRouterWithNewServer);
    Assert.assertEquals(lastTxIdInNN, lastTxIdInRouter + 1);
  }

  @Test
  public void testRBFDisableAndNNDisable() throws IOException {
    createCluster(false, false);
    RouterContext routerContext = cluster.getRandomRouter();
    FileSystem fileSystem = routerContext.getFileSystem();
    RouterRpcClient routerRpcClient = routerContext.getRouterRpcClient();
    ConnectionManager connectionManager = routerRpcClient.getConnectionManager();
    NameNode activeNameNode = cluster.getNamenodes().get(0).getNamenode();
    String nsId = cluster.getRandomNamenode().getNameserviceId();

    Path testPath =  new Path("/test_verify_new_msync_server");
    FSDataOutputStream outputStream = fileSystem.create(testPath);
    outputStream.write("hello world".getBytes());
    outputStream.close();

    long lastTxIdInNN = activeNameNode.getNamesystem().getFSImage().getLastAppliedOrWrittenTxId();

    fileSystem.getFileStatus(testPath);

    AlignmentContext alignmentContext = connectionManager.getNsAlignmentContext(nsId);
    long lastTxIdInRouter = alignmentContext.getLastSeenStateId();
    Assert.assertEquals(lastTxIdInNN, lastTxIdInRouter);

    boolean setReplication = activeNameNode.getRpcServer().setReplication(
        testPath.toUri().getPath(), (short) 4);
    Assert.assertTrue(setReplication);

    routerRpcClient.internalMsync(UserGroupInformation.getCurrentUser(), nsId);

    long lastTxIdInRouterWithNewServer = alignmentContext.getLastSeenStateId();
    lastTxIdInNN = activeNameNode.getNamesystem().getFSImage().getLastAppliedOrWrittenTxId();

    Assert.assertEquals(lastTxIdInNN, lastTxIdInRouterWithNewServer);
    Assert.assertEquals(lastTxIdInNN, lastTxIdInRouter + 1);
  }
}
