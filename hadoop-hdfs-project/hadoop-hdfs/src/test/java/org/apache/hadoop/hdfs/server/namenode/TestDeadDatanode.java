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
package org.apache.hadoop.hdfs.server.namenode;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.HdfsBlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.protocol.SlowDiskReports;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.InternalDataNodeTestUtils;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.RegisterCommand;
import org.apache.hadoop.hdfs.server.protocol.SlowPeerReports;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Test;

/**
 * Test to ensure requests from dead datnodes are rejected by namenode with
 * appropriate exceptions/failure response
 */
public class TestDeadDatanode {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestDeadDatanode.class);
  private MiniDFSCluster cluster;

  @After
  public void cleanup() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * Test to ensure namenode rejects request from dead datanode
   * - Start a cluster
   * - Shutdown the datanode and wait for it to be marked dead at the namenode
   * - Send datanode requests to Namenode and make sure it is rejected 
   *   appropriately.
   */
  @Test
  public void testDeadDatanode() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 500);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitActive();

    String poolId = cluster.getNamesystem().getBlockPoolId();
    // wait for datanode to be marked live
    DataNode dn = cluster.getDataNodes().get(0);
    DatanodeRegistration reg = InternalDataNodeTestUtils.
      getDNRegistrationForBP(cluster.getDataNodes().get(0), poolId);

    DFSTestUtil.waitForDatanodeState(cluster, reg.getDatanodeUuid(), true, 20000);

    // Shutdown and wait for datanode to be marked dead
    dn.shutdown();
    DFSTestUtil.waitForDatanodeState(cluster, reg.getDatanodeUuid(), false, 20000);

    DatanodeProtocol dnp = cluster.getNameNodeRpc();
    
    ReceivedDeletedBlockInfo[] blocks = { new ReceivedDeletedBlockInfo(
        new Block(0), 
        ReceivedDeletedBlockInfo.BlockStatus.RECEIVED_BLOCK,
        null) };
    StorageReceivedDeletedBlocks[] storageBlocks = { 
        new StorageReceivedDeletedBlocks(
            new DatanodeStorage(reg.getDatanodeUuid()), blocks) };

    // Ensure blockReceived call from dead datanode is not rejected with
    // IOException, since it's async, but the node remains unregistered.
    dnp.blockReceivedAndDeleted(reg, poolId, storageBlocks);
    BlockManager bm = cluster.getNamesystem().getBlockManager();
    // IBRs are async, make sure the NN processes all of them.
    bm.flushBlockOps();
    assertFalse(bm.getDatanodeManager().getDatanode(reg).isRegistered());

    // Ensure blockReport from dead datanode is rejected with IOException
    StorageBlockReport[] report = { new StorageBlockReport(
        new DatanodeStorage(reg.getDatanodeUuid()),
        BlockListAsLongs.EMPTY) };
    try {
      dnp.blockReport(reg, poolId, report,
          new BlockReportContext(1, 0, System.nanoTime(), 0L));
      fail("Expected IOException is not thrown");
    } catch (IOException ex) {
      // Expected
    }

    // Ensure heartbeat from dead datanode is rejected with a command
    // that asks datanode to register again
    StorageReport[] rep = { new StorageReport(
        new DatanodeStorage(reg.getDatanodeUuid()),
        false, 0, 0, 0, 0, 0) };
    DatanodeCommand[] cmd =
        dnp.sendHeartbeat(reg, rep, 0L, 0L, 0, 0, 0, null, true,
            SlowPeerReports.EMPTY_REPORT, SlowDiskReports.EMPTY_REPORT, 0, 0, 0)
            .getCommands();
    assertEquals(1, cmd.length);
    assertEquals(cmd[0].getAction(), RegisterCommand.REGISTER
        .getAction());
  }

  @Test
  public void testDeadNodeAsBlockTarget() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 500);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();

    String poolId = cluster.getNamesystem().getBlockPoolId();
    // wait for datanode to be marked live
    DataNode dn = cluster.getDataNodes().get(0);
    DatanodeRegistration reg = InternalDataNodeTestUtils.
        getDNRegistrationForBP(cluster.getDataNodes().get(0), poolId);
    // Get the updated datanode descriptor
    BlockManager bm = cluster.getNamesystem().getBlockManager();
    DatanodeManager dm = bm.getDatanodeManager();
    Node clientNode = dm.getDatanode(reg);

    DFSTestUtil.waitForDatanodeState(cluster, reg.getDatanodeUuid(), true,
        20000);

    // Shutdown and wait for datanode to be marked dead
    dn.shutdown();
    DFSTestUtil.waitForDatanodeState(cluster, reg.getDatanodeUuid(), false,
        20000);
    // Get the updated datanode descriptor available in DNM
    // choose the targets, but local node should not get selected as this is not
    // part of the cluster anymore
    DatanodeStorageInfo[] results = bm.chooseTarget4NewBlock("/hello", 3,
        clientNode, new HashSet<>(), 256 * 1024 * 1024L, null, (byte) 7,
        BlockType.CONTIGUOUS, null, null);
    for (DatanodeStorageInfo datanodeStorageInfo : results) {
      assertFalse("Dead node should not be chosen", datanodeStorageInfo
          .getDatanodeDescriptor().equals(clientNode));
    }
  }

  @Test
  public void testDeadNodeProtectedByFaultyDC() throws Exception {
    Configuration conf = new HdfsConfiguration();
    final String[] racks = {"/datacenter1/rack0", "/datacenter0/rack1",
        "/datacenter0/rack2", "/datacenter0/rack3"};
    final String[] hosts = {"host0", "host1", "host2", "host3"};
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY, 100);
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(racks.length).racks(racks)
          .hosts(hosts).build();
      cluster.waitActive();
      BlockManager blockManager = cluster.getNamesystem(0).getBlockManager();
      DistributedFileSystem fs = cluster.getFileSystem();

      DataNode dn1 = null;
      DataNode dn2 = null;
      DataNode dn4 = null;
      for (DataNode node : cluster.getDataNodes()) {
        if (node.getDatanodeHostname().contains("host0")) {
          dn1 = node;
        } else if (node.getDatanodeHostname().contains("host1")) {
          dn2 = node;
        } else if (node.getDatanodeHostname().contains("host3")) {
          dn4 = node;
        }
      }

      // Stop DN4, so that the block can be stored from dn1 ~ dn3.
      dn4.setHeartbeatsDisabledForTests(true);
      cluster.setDataNodeDead(dn4.getDatanodeId());

      Path filePath = new Path("/tmp/testDeadNodeProtectedByFaultyDC");
      DFSTestUtil.createFile(fs, filePath, 1024, (short) 3, 0);

      BlockLocation[] blockLocations = fs.getClient().getBlockLocations(
          filePath.toUri().getPath(), 0, Long.MAX_VALUE);
      assertEquals(1, blockLocations.length);
      ExtendedBlock block = ((HdfsBlockLocation) blockLocations[0]).getLocatedBlock().getBlock();

      final DatanodeDescriptor storeDN1 = blockManager.getDatanodeManager()
          .getDatanode(dn1.getDatanodeId());
      assertEquals(1, storeDN1.numBlocks());

      // Mark the DN1 as the faulty DN
      blockManager.setFaultyDC("/datacenter1");
      assertTrue(storeDN1.isProtectedByFaultyDC());

      // Start the DN4.
      dn4.setHeartbeatsDisabledForTests(false);
      final DatanodeDescriptor storeDN4 = blockManager.getDatanodeManager()
          .getDatanode(dn4.getDatanodeId());
      GenericTestUtils.waitFor(
          () -> storeDN4.isAlive() && storeDN4.isHeartbeatedSinceRegistration(), 100, 5000);

      // The replicas of this block still be stored from DN1 ~ DN3
      BlockInfo blockInfo = blockManager.getStoredBlock(block.getLocalBlock());
      Iterator<DatanodeStorageInfo> storageInfoIterator = blockInfo.getStorageInfos();
      AtomicInteger counter = new AtomicInteger();
      storageInfoIterator.forEachRemaining(k -> counter.getAndIncrement());
      assertEquals(3, counter.get());
      blockLocations = fs.getClient().getBlockLocations(
          filePath.toUri().getPath(), 0, Long.MAX_VALUE);
      assertEquals(2, blockLocations[0].getHosts().length);
      assertFalse(Arrays.asList(blockLocations[0].getHosts()).contains("host0"));

      // Stop the DN2, normally namenode will handle this under replicated block.
      final DatanodeDescriptor storeDN2 = blockManager.getDatanodeManager()
          .getDatanode(dn2.getDatanodeId());
      dn2.setHeartbeatsDisabledForTests(true);
      cluster.setDataNodeDead(dn2.getDatanodeId());
      assertFalse(storeDN2.isProtectedByFaultyDC());

      BlockManagerTestUtil.getComputedDatanodeWork(blockManager);

      GenericTestUtils.waitFor(() -> {
        BlockInfo tmpBlockInfo = blockManager.getStoredBlock(block.getLocalBlock());
        Iterator<DatanodeStorageInfo> tmpStorageInfoIterator = tmpBlockInfo.getStorageInfos();
        AtomicInteger tmpCounter = new AtomicInteger(0);
        tmpStorageInfoIterator.forEachRemaining(k -> tmpCounter.getAndIncrement());
        return 3 == tmpCounter.get();
      }, 3000, 20000);

      // Get BlockLocation will ignore the maintenance replicas
      blockLocations = fs.getClient().getBlockLocations(
          filePath.toUri().getPath(), 0, Long.MAX_VALUE);
      assertEquals(1, blockLocations.length);
      assertEquals(2, blockLocations[0].getHosts().length);
      assertTrue(Arrays.asList(blockLocations[0].getHosts()).contains("host2"));
      assertTrue(Arrays.asList(blockLocations[0].getHosts()).contains("host3"));

      // Restart the DN2, NN should process excess replicas.
      dn2.setHeartbeatsDisabledForTests(false);
      GenericTestUtils.waitFor(
          () -> storeDN2.isAlive() && storeDN2.isHeartbeatedSinceRegistration(), 100, 5000);

      blockInfo = blockManager.getStoredBlock(block.getLocalBlock());
      storageInfoIterator = blockInfo.getStorageInfos();
      counter.set(0);
      storageInfoIterator.forEachRemaining(k -> counter.getAndIncrement());
      assertEquals(4, counter.get());

      blockLocations = fs.getClient().getBlockLocations(
          filePath.toUri().getPath(), 0, Long.MAX_VALUE);
      assertEquals(1, blockLocations.length);
      assertEquals(3, blockLocations[0].getHosts().length);
      assertTrue(Arrays.asList(blockLocations[0].getHosts()).contains("host1"));
      assertTrue(Arrays.asList(blockLocations[0].getHosts()).contains("host2"));
      assertTrue(Arrays.asList(blockLocations[0].getHosts()).contains("host3"));

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testFaultyDCMonitor() throws Exception {
    Configuration conf = new HdfsConfiguration();
    final String[] racks = {"/datacenter1/rack0", "/datacenter0/rack1",
        "/datacenter0/rack2", "/datacenter0/rack3"};
    final String[] hosts = {"host0", "host1", "host2", "host3"};
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_FAULTY_DC_NUMBER_THRESHOLD_KEY, 2);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 0);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ENABLE_FAULTY_DC_MONITOR_KEY, true);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY, Long.MAX_VALUE);

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(racks.length).racks(racks)
          .hosts(hosts).build();
      cluster.waitActive();
      BlockManager blockManager = cluster.getNamesystem(0).getBlockManager();

      DataNode dn2 = null;
      DataNode dn3 = null;
      DataNode dn4 = null;
      for (DataNode node : cluster.getDataNodes()) {
        if (node.getDatanodeHostname().contains("host1")) {
          dn2 = node;
        } else if (node.getDatanodeHostname().contains("host2")) {
          dn3 = node;
        } else if (node.getDatanodeHostname().contains("host3")) {
          dn4 = node;
        }
      }

      // Holding the heartbeat of DN2 and DN4
      dn2.setHeartbeatsDisabledForTests(true);
      dn4.setHeartbeatsDisabledForTests(true);

      blockManager.setFaultyDCTimeThresholdMs(2500);
      assertEquals(2500, blockManager.getFaultyDCTimeThresholdMs());
      GenericTestUtils.waitFor(() -> blockManager.getFaultyDC() != null, 500, 6000);
      assertEquals("/datacenter0", blockManager.getFaultyDC());

      DatanodeDescriptor dn2Desc = cluster.getNamesystem(0)
          .getBlockManager().getDatanodeManager()
          .getDatanode(dn2.getDatanodeId());
      assertTrue(dn2Desc.isProtectedByFaultyDC());

      DatanodeDescriptor dn3Desc = blockManager.getDatanodeManager()
          .getDatanode(dn3.getDatanodeId());
      assertTrue(dn3Desc.isProtectedByFaultyDC());

      DatanodeDescriptor dn4Desc = blockManager.getDatanodeManager()
          .getDatanode(dn4.getDatanodeId());
      assertTrue(dn4Desc.isProtectedByFaultyDC());

      cluster.setDataNodeDead(dn2.getDatanodeId());
      cluster.setDataNodeDead(dn4.getDatanodeId());

      blockManager.setFaultyDC(null);

      // DN2 and DN4 still be protected by the FaultyDC since there are dead.
      assertNull(blockManager.getFaultyDC());
      dn2Desc = blockManager.getDatanodeManager()
          .getDatanode(dn2.getDatanodeId());
      assertTrue(dn2Desc.isProtectedByFaultyDC());

      dn4Desc = blockManager.getDatanodeManager()
          .getDatanode(dn4.getDatanodeId());
      assertTrue(dn4Desc.isProtectedByFaultyDC());

      // Restart the DN2, NN should process excess replicas.
      dn2.setHeartbeatsDisabledForTests(false);
      DatanodeDescriptor finalDn2Desc = dn2Desc;
      GenericTestUtils.waitFor(
          () -> finalDn2Desc.isAlive() && finalDn2Desc.isHeartbeatedSinceRegistration(), 100, 5000);

      dn2Desc = blockManager.getDatanodeManager().getDatanode(dn2.getDatanodeId());
      assertFalse(dn2Desc.isProtectedByFaultyDC());

      // Restart the DN4, NN should process excess replicas.
      dn4.setHeartbeatsDisabledForTests(false);
      DatanodeDescriptor finalDn4Desc = dn4Desc;
      GenericTestUtils.waitFor(
          () -> finalDn4Desc.isAlive() && finalDn4Desc.isHeartbeatedSinceRegistration(), 100, 5000);
      dn4Desc = blockManager.getDatanodeManager().getDatanode(dn4.getDatanodeId());
      assertFalse(dn4Desc.isProtectedByFaultyDC());

      assertEquals(3, blockManager.getNumberOfPendingScanDN());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testNonDFSUsedONDeadNodeReReg() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
        3000);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY,
        6 * 1000);
    long CAPACITY = 5000L;
    long[] capacities = new long[] { 4 * CAPACITY, 4 * CAPACITY };
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2)
          .simulatedCapacities(capacities).build();
      long initialCapacity = cluster.getNamesystem(0).getCapacityTotal();
      assertTrue(initialCapacity > 0);
      DataNode dn1 = cluster.getDataNodes().get(0);
      DataNode dn2 = cluster.getDataNodes().get(1);
      final DatanodeDescriptor dn2Desc = cluster.getNamesystem(0)
          .getBlockManager().getDatanodeManager()
          .getDatanode(dn2.getDatanodeId());
      dn1.setHeartbeatsDisabledForTests(true);
      cluster.setDataNodeDead(dn1.getDatanodeId());
      assertEquals("Capacity shouldn't include DeadNode", dn2Desc.getCapacity(),
          cluster.getNamesystem(0).getCapacityTotal());
      assertEquals("NonDFS-used shouldn't include DeadNode",
          dn2Desc.getNonDfsUsed(),
          cluster.getNamesystem(0).getNonDfsUsedSpace());
      // Wait for re-registration and heartbeat
      dn1.setHeartbeatsDisabledForTests(false);
      final DatanodeDescriptor dn1Desc = cluster.getNamesystem(0)
          .getBlockManager().getDatanodeManager()
          .getDatanode(dn1.getDatanodeId());
      GenericTestUtils.waitFor(new Supplier<Boolean>() {

        @Override public Boolean get() {
          return dn1Desc.isAlive() && dn1Desc.isHeartbeatedSinceRegistration();
        }
      }, 100, 5000);
      assertEquals("Capacity should be 0 after all DNs dead", initialCapacity,
          cluster.getNamesystem(0).getCapacityTotal());
      long nonDfsAfterReg = cluster.getNamesystem(0).getNonDfsUsedSpace();
      assertEquals("NonDFS should include actual DN NonDFSUsed",
          dn1Desc.getNonDfsUsed() + dn2Desc.getNonDfsUsed(), nonDfsAfterReg);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
