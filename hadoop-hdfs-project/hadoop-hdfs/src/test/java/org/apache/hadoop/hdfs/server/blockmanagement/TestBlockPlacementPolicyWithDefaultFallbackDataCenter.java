/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.net.DFSNetworkTopology;
import org.apache.hadoop.hdfs.net.DFSNetworkTopologyWithDataCenter;
import org.apache.hadoop.hdfs.net.NetworkTopologyUtil;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.zoneservice.ReplicationRule;
import org.apache.hadoop.net.StaticMapping;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestBlockPlacementPolicyWithDefaultFallbackDataCenter
    extends TestBlockPlacementPolicyWithDataCenter {

  private final String DEFAULT_DC = "datacenter0";

  @Before
  public void setup() throws IOException {
    StaticMapping.resetMap();
    Configuration conf = new HdfsConfiguration();
    final String[] racks = {
        "/datacenter0/rack0", "/datacenter0/rack0", "/datacenter0/rack1",
        "/datacenter0/rack1", "/datacenter1/rack0", "/datacenter1/rack1",
        "/datacenter0/rack0", "/datacenter1/rack0", "/datacenter1/rack0",
        "/datacenter2/rack0", "/datacenter2/rack0", "/datacenter2/rack0",
        "/datacenter3/rack0", "/datacenter4/rack0", "/datacenter5/rack0",
        "/datacenter5/rack0",
        "/datacenter255/rack0", "/datacenter255/rack1", "/datacenter255/rack2"};
    final String[] hosts = {
        "host0", "host1", "host2", "host3", "host4", "host5", "host6",
        "host7", "host8", "host9", "host10", "host11", "host12", "host13",
        "host14", "host15",
        "host255-0", "host255-1", "host255-2"};

    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE / 2);
    conf.setClass(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
        BlockPlacementPolicyWithDefaultFallbackDataCenter.class,
        BlockPlacementPolicy.class);
    conf.setBoolean(DFSConfigKeys.DFS_USE_DFS_NETWORK_TOPOLOGY_KEY, true);
    conf.setClass(DFSConfigKeys.DFS_NET_TOPOLOGY_IMPL_KEY,
        DFSNetworkTopologyWithDataCenter.class, DFSNetworkTopology.class);
    conf.set(DFSConfigKeys.DFS_NAMENODE_BLOCK_PLACEMENT_POLICY_WITH_DATA_CENTER_FALLBACK_DC_KEY,
        DEFAULT_DC);
    conf.setBoolean(CommonConfigurationKeys.IGNORE_SDI_AUTHENTICATE_KEY, true);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY, REPLICATION_FACTOR);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(racks.length).racks(racks)
        .hosts(hosts).build();
    cluster.waitActive();
    nameNodeRpc = cluster.getNameNodeRpc();
    namesystem = cluster.getNamesystem();
    perm = new PermissionStatus("TestBlockPlacementPolicyWithDataCenter", null,
        FsPermission.getDefault());
  }

  @Test
  @Override
  public void testDatacenterOffPlacement() {
    String clientMachine = "client.foo.com";
    String clientRack = "/datacenter9/rack0";
    StaticMapping.addNodeToRack(clientMachine, clientRack);
    for (int i = 0; i < times; i++) {
      LOG.info("Start round " + i + " ...");
      try {
        DatanodeInfo[] locations = getLocations(clientMachine);
        verifyDNinDC(DEFAULT_DC, locations[0]);
        verifyDNinDC(DEFAULT_DC, locations[1]);
        verifyDNinDC(DEFAULT_DC, locations[2]);
      } catch (IOException e) {
        e.printStackTrace();
        fail("No exception should be thrown");
      }
    }
  }

  @Test
  public void testNotEnoughDNsInDefaultDC() throws IOException {
    // Set default fallback DC to datacenter3, which has only 1 DN.
    BlockPlacementPolicyWithDefaultFallbackDataCenter policy =
        (BlockPlacementPolicyWithDefaultFallbackDataCenter) namesystem.getBlockManager()
            .getBlockPlacementPolicy();
    policy.setDefaultDC("datacenter3");

    // Client from dc9 tries to add block. Fall back to dc3, only 1 replica found.
    verifyAddBlock("datacenter9", 1, true);
    // Client from dc4 tries to add block. No fall back because 1 replica found on dc4.
    verifyAddBlock("datacenter4", 1, true);
    // Client from dc5 tries to add block. No fall back because 2 replica found on dc4.
    verifyAddBlock("datacenter5", 2, true);

    namesystem.getBlockManager().setAlertInsufficientTargetsEnabled(true);
    GenericTestUtils.LogCapturer log = GenericTestUtils.LogCapturer.captureLogs(
        LoggerFactory.getLogger(BlockManager.class));
    // dc255 has 3 nodes, adding 4 or more will log a warning
    log.clearOutput();
    verifyAddBlockSuccessfully("datacenter255", 4, 3);
    assertTrue(log.getOutput().contains("Failed to fully assign targets: 3 out of 4"));
    log.clearOutput();
    verifyAddBlockSuccessfully("datacenter255", 5, 3);
    assertTrue(log.getOutput().contains("Failed to fully assign targets: 3 out of 5"));
    namesystem.getBlockManager().setAlertInsufficientTargetsEnabled(false);
  }

  @Test
  @Override
  public void testChooseTarget() {
    BlockPlacementPolicyWithDataCenter policy =
        (BlockPlacementPolicyWithDataCenter) namesystem.getBlockManager()
            .getBlockPlacementPolicy();
    BlockStoragePolicy storagePolicy =
        BlockStoragePolicySuite.createDefaultSuite().getDefaultPolicy();

    // Sort datanodes by rack, and add the DatanodeStorageInfo to map.
    Set<DatanodeDescriptor> datanodes =
        namesystem.getBlockManager().getDatanodeManager().getDatanodes();
    Map<String, List<DatanodeStorageInfo>> dcMap =
        getDcMapFromDatanodes(datanodes);

    // replicate to the datacenter without any replicas first
    ReplicationRule rule =
        ReplicationRule.parseFromString("/datacenter0:2,/datacenter1:2");
    List<DatanodeStorageInfo> existNodes = new ArrayList<>();
    existNodes.add(dcMap.get("/datacenter0/rack0").get(0));
    DatanodeStorageInfo[] results =
        policy.chooseTarget(null, 3, rule, null, existNodes, false, null,
            DEFAULT_BLOCK_SIZE, storagePolicy, null);
    assertEquals(1, results.length);
    for (DatanodeStorageInfo info : results) {
      assertEquals("/datacenter1",
          NetworkTopologyUtil.getDataCenter(info.getDatanodeDescriptor()));
    }

    // only replicate to one datacenter one time
    existNodes.clear();
    existNodes.add(dcMap.get("/datacenter0/rack0").get(0));
    existNodes.add(dcMap.get("/datacenter1/rack0").get(0));
    results = policy.chooseTarget(null, 2, rule, null, existNodes, false, null,
        DEFAULT_BLOCK_SIZE, storagePolicy, null);
    assertEquals(1, results.length);
    String dc =
        NetworkTopologyUtil.getDataCenter(results[0].getDatanodeDescriptor());
    assertTrue(dc.equals("/datacenter0") || dc.equals("/datacenter1"));

    // allocated should not beyond numOfReplicas
    rule = ReplicationRule.parseFromString("/datacenter0:3");
    existNodes.clear();
    existNodes.add(dcMap.get("/datacenter0/rack0").get(0));
    results = policy.chooseTarget(null, 1, rule, null, existNodes, false, null,
        DEFAULT_BLOCK_SIZE, storagePolicy, null);
    assertEquals(1, results.length);
    dc = NetworkTopologyUtil.getDataCenter(results[0].getDatanodeDescriptor());
    assertEquals("/datacenter0", dc);

    // replica=0 case
    rule = ReplicationRule.parseFromString("/datacenter0:0");
    existNodes.clear();
    existNodes.add(dcMap.get("/datacenter1/rack0").get(0));
    results = policy.chooseTarget(null, 1, rule, null, existNodes, false, null,
        DEFAULT_BLOCK_SIZE, storagePolicy, null);
    assertEquals(0, results.length);

    // datacenter does not exist, fallback to default
    rule = ReplicationRule.parseFromString("/datacenter9:2");
    results = policy.chooseTarget(null, 1, rule, null, existNodes, false, null,
        DEFAULT_BLOCK_SIZE, storagePolicy, null);
    assertEquals(1, results.length);
  }

  private void verifyDNinDC(String datacenter, DatanodeInfo datanodeInfo) {
    assertEquals("/" + datacenter,
        NetworkTopologyUtil.getDataCenter(datanodeInfo));
  }

  private void verifyAddBlockSuccessfully(String clientDC, int expectedReplicas,
      int expectedFinalReplicas) throws IOException {
    final String clientMachine = "client.foo.com";
    String clientRack = "/" + clientDC + "/rack0";
    StaticMapping.addNodeToRack(clientMachine, clientRack);
    final String src = "/test" + Time.now();

    HdfsFileStatus fileStatus =
        namesystem.startFile(src, perm, clientMachine, clientMachine, EnumSet.of(CreateFlag.CREATE),
            true, (short) expectedReplicas, DEFAULT_BLOCK_SIZE, null, null, null, false);
    LocatedBlock locatedBlock = nameNodeRpc.addBlock(src, clientMachine, null, null, fileStatus.getFileId(), null, null);
    assertEquals(expectedFinalReplicas, locatedBlock.getLocations().length);
  }

  private void verifyAddBlock(String clientDC, int expectedReplicas, boolean failed) {
    final String clientMachine = "client.foo.com";
    String clientRack = "/" + clientDC + "/rack0";
    StaticMapping.addNodeToRack(clientMachine, clientRack);
    final String src = "/test" + Time.now();

    // Try creating file on a non-existent DC
    // Falling back to datacenter3 fails because there are not enough DNs for
    // replication factor.
    try {
      HdfsFileStatus fileStatus = namesystem.startFile(src, perm, clientMachine, clientMachine,
          EnumSet.of(CreateFlag.CREATE), true, REPLICATION_FACTOR,
          DEFAULT_BLOCK_SIZE, null, null, null, false);
      LocatedBlock locatedBlock = nameNodeRpc.addBlock(src, clientMachine, null, null,
          fileStatus.getFileId(), null, null);
      if (failed) {
        fail("Adding block should fail.");
      } else {
        assertEquals(REPLICATION_FACTOR, locatedBlock.getLocations().length);
        nameNodeRpc.abandonBlock(locatedBlock.getBlock(), fileStatus.getFileId(),
            src, clientMachine);
      }
    } catch (IOException e) {
      if (failed) {
        String errMsg = "File " + src + " could only be written to " + expectedReplicas
            + " of the " + REPLICATION_FACTOR + " minReplication nodes.";
        assertTrue(e.getMessage().contains(errMsg));
      } else {
        fail("Adding block should not fail.");
      }
    }
  }

  @Test
  public void testChooseTargetForDR() throws IOException, InterruptedException, TimeoutException {
    Configuration conf = new HdfsConfiguration();
    final String[] racks = {"/datacenter0/rack0", "/datacenter0/rack0", "/datacenter0/rack1"};
    final String[] hosts = {"host0", "host1", "host2"};
    long drColdDataThresholdMS = 30000;
    initConfForDr(conf, drColdDataThresholdMS, "datacenter0");
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).
        racks(racks).hosts(hosts).build()) {
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      BlockManager blockManager = cluster.getNamesystem().getBlockManager();
      // Create test file.
      Path file1 = new Path("/test1");
      DFSTestUtil.createFile(fs, file1, 1024L, (short) 2, 0L);
      DFSTestUtil.waitReplication(fs, file1, (short) 2);
      LocatedBlock lb = DFSTestUtil.getAllBlocks(fs, file1).get(0);
      DatanodeInfo[] loc = lb.getLocations();
      assertEquals(2, loc.length);
      Map<String, Integer> idcMap = countDataNodeDC(loc);
      // The replication rule is /datacenter0:2.
      assertEquals(2, idcMap.get("/datacenter0").intValue());

      Path file2 = new Path("/test2");
      DFSTestUtil.createFile(fs, file2, 1024L, (short)3, 0L);
      DFSTestUtil.waitReplication(fs, file2, (short) 3);
      lb = DFSTestUtil.getAllBlocks(fs, file2).get(0);
      loc = lb.getLocations();
      assertEquals(3, loc.length);
      idcMap = countDataNodeDC(loc);
      // The replication rule is /datacenter0:3.
      assertEquals(3, idcMap.get("/datacenter0").intValue());

      // Adding 3 new hosts about '/datacenter1'.
      cluster.startDataNodes(conf, 3, true, null,
          new String[]{"/datacenter1/rack0", "/datacenter1/rack0", "/datacenter1/rack1"},
          new String[]{"host3", "host4", "host5"},
          null);
      cluster.triggerHeartbeats();

      // Set file1 increase 3 replication.
      fs.setReplication(file1, (short) 3);
      DFSTestUtil.waitReplication(fs, file1, (short) 3);
      lb = DFSTestUtil.getAllBlocks(fs, file1).get(0);
      loc = lb.getLocations();
      assertEquals(3, loc.length);
      idcMap = countDataNodeDC(loc);
      // The replication rule is /datacenter0:2,/datacenter1:1
      assertEquals(2, idcMap.get("/datacenter0").intValue());
      assertEquals(1, idcMap.get("/datacenter1").intValue());

      // Set file1 increase 4 replication.
      fs.setReplication(file1, (short) 4);
      DFSTestUtil.waitReplication(fs, file1, (short) 4);
      lb = DFSTestUtil.getAllBlocks(fs, file1).get(0);
      loc = lb.getLocations();
      assertEquals(4, loc.length);
      idcMap = countDataNodeDC(loc);
      // The replication rule is /datacenter0:2,/datacenter1:2
      assertEquals(2, idcMap.get("/datacenter0").intValue());
      assertEquals(2, idcMap.get("/datacenter1").intValue());

      // Mock the file to become cold data.
      blockManager.setGenerateDrRuleForTest(true);

      // Set file1 decrease 3 replication.
      fs.setReplication(file1, (short) 3);
      DFSTestUtil.waitReplication(fs, file1, (short) 3);
      lb = DFSTestUtil.getAllBlocks(fs, file1).get(0);
      loc = lb.getLocations();
      assertEquals(3, loc.length);
      // The current setting configuration dfs.namenode.replication-rule.cold-data.for.dr is
      // "3=/datacenter0:1,/datacenter1:2",
      // so the replication rule is /datacenter0:1,/datacenter1:2.
      idcMap = countDataNodeDC(loc);
      assertEquals(1, idcMap.get("/datacenter0").intValue());
      assertEquals(2, idcMap.get("/datacenter1").intValue());

      // Set file1 decrease 2 replication.
      fs.setReplication(file1, (short) 2);
      DFSTestUtil.waitReplication(fs, file1, (short) 2);
      lb = DFSTestUtil.getAllBlocks(fs, file1).get(0);
      loc = lb.getLocations();
      assertEquals(2, loc.length);
      // The replication rule is /datacenter0:1,/datacenter1:1.
      idcMap = countDataNodeDC(loc);
      assertEquals(1, idcMap.get("/datacenter0").intValue());
      assertEquals(1, idcMap.get("/datacenter1").intValue());

      // Set file2 increase 6 replication.
      fs.setReplication(file2, (short) 6);
      DFSTestUtil.waitReplication(fs, file2, (short) 6);
      lb = DFSTestUtil.getAllBlocks(fs, file2).get(0);
      loc = lb.getLocations();
      assertEquals(6, loc.length);
      idcMap = countDataNodeDC(loc);
      // The replication rule is /datacenter0:3,/datacenter1:3
      assertEquals(3, idcMap.get("/datacenter0").intValue());
      assertEquals(3, idcMap.get("/datacenter1").intValue());

      // Set file2 decrease 2 replication.
      fs.setReplication(file2, (short) 2);
      DFSTestUtil.waitReplication(fs, file2, (short) 2);
      lb = DFSTestUtil.getAllBlocks(fs, file2).get(0);
      loc = lb.getLocations();
      assertEquals(2, loc.length);
      idcMap = countDataNodeDC(loc);
      // The replication rule is /datacenter0:1,/datacenter1:1
      assertEquals(1, idcMap.get("/datacenter0").intValue());
      assertEquals(1, idcMap.get("/datacenter1").intValue());

      blockManager.setGenerateDrRuleForTest(false);
    }
  }

  @Test
  public void testChooseTargetInvalidDataCenterForDR() throws IOException,
      InterruptedException, TimeoutException {
    Configuration conf = new HdfsConfiguration();
    final String[] racks = {"/datacenter2/rack0", "/datacenter2/rack0", "/datacenter2/rack1"};
    final String[] hosts = {"host0", "host1", "host2"};
    long drColdDataThresholdMS = 30000;

    // Current config `dfs.namenode.valid.datacenters.for.dr` is ”/datacenter0,/datacenter1",
    // "/datacenter2" is invalid datacenter for DR, so will not run choose target logic about DR.
    initConfForDr(conf, drColdDataThresholdMS, "datacenter2");

    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).
        racks(racks).hosts(hosts).build()) {
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      BlockManager blockManager = cluster.getNamesystem().getBlockManager();
      // Create test file.
      Path file = new Path("/test");
      DFSTestUtil.createFile(fs, file, 1024L, (short) 2, 0L);
      DFSTestUtil.waitReplication(fs, file, (short) 2);
      LocatedBlock lb = DFSTestUtil.getAllBlocks(fs, file).get(0);
      DatanodeInfo[] loc = lb.getLocations();
      assertEquals(2, loc.length);
      String expectedDC = "datacenter2";
      verifyDNinDC(expectedDC, loc[0]);
      verifyDNinDC(expectedDC, loc[1]);

      // Set increase 3 replication and will not run choose target logic about DR.
      fs.setReplication(file, (short) 3);
      DFSTestUtil.waitReplication(fs, file, (short) 3);
      lb = DFSTestUtil.getAllBlocks(fs, file).get(0);
      loc = lb.getLocations();
      assertEquals(3, loc.length);
      verifyDNinDC(expectedDC, loc[0]);
      verifyDNinDC(expectedDC, loc[1]);
      verifyDNinDC(expectedDC, loc[2]);

      // Mock the file to become cold data.
      blockManager.setGenerateDrRuleForTest(true);

      // Set decrease 2 replication and will not run choose target logic about DR.
      fs.setReplication(file, (short) 2);
      DFSTestUtil.waitReplication(fs, file, (short) 2);
      lb = DFSTestUtil.getAllBlocks(fs, file).get(0);
      loc = lb.getLocations();
      assertEquals(2, loc.length);
      verifyDNinDC(expectedDC, loc[0]);
      verifyDNinDC(expectedDC, loc[1]);

      blockManager.setGenerateDrRuleForTest(false);
    }
  }

  private void initConfForDr(Configuration conf, long drColdDataThresholdMS,
      String defaultDataCenter) {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setClass(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
        BlockPlacementPolicyWithDefaultFallbackDataCenter.class,
        BlockPlacementPolicy.class);
    conf.setBoolean(DFSConfigKeys.DFS_USE_DFS_NETWORK_TOPOLOGY_KEY, true);
    conf.setClass(DFSConfigKeys.DFS_NET_TOPOLOGY_IMPL_KEY,
        DFSNetworkTopologyWithDataCenter.class, DFSNetworkTopology.class);
    conf.set(DFSConfigKeys.DFS_NAMENODE_BLOCK_PLACEMENT_POLICY_WITH_DATA_CENTER_FALLBACK_DC_KEY,
        defaultDataCenter);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 500);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY, 10);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_DR_REPLICATION_RULE_ENABLE_KEY, true);
    conf.set(DFSConfigKeys.DFS_NAMENODE_DR_DATACENTERS_KEY, "/datacenter0,/datacenter1");
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_DR_COLD_DATA_THRESHOLD_MS_KEY,
        drColdDataThresholdMS);
    conf.set(DFSConfigKeys.DFS_NAMENODE_DR_REPLICATION_RULE_COLD_DATA_KEY,
        "3=/datacenter0:1,/datacenter1:2");
  }

  private Map<String, Integer> countDataNodeDC(DatanodeInfo[] loc) {
    Map<String, Integer> dcMap = new HashMap<>();
    for (DatanodeInfo dn : loc) {
      String dcName = NetworkTopologyUtil.getDataCenter(dn);
      dcMap.put(dcName, dcMap.getOrDefault(dcName, 0) + 1);
    }
    return dcMap;
  }
}
