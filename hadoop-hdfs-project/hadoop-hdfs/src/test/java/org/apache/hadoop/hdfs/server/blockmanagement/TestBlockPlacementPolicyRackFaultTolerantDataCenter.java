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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.net.DFSNetworkTopology;
import org.apache.hadoop.hdfs.net.DFSNetworkTopologyWithDataCenter;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.net.StaticMapping;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.thirdparty.com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestBlockPlacementPolicyRackFaultTolerantDataCenter {

  protected static final Logger LOG =
      LoggerFactory.getLogger(TestBlockPlacementPolicyRackFaultTolerantDataCenter.class);
  private static final int DEFAULT_BLOCK_SIZE = 1024;
  private MiniDFSCluster cluster = null;
  private NamenodeProtocols nameNodeRpc = null;
  private FSNamesystem namesystem = null;
  private PermissionStatus perm = null;
  private final String DEFAULT_DC = "datacenter0";
  protected final int times = 10;

  @Before
  public void setup() throws IOException {
    StaticMapping.resetMap();
    Configuration conf = new HdfsConfiguration();
    final ArrayList<String> rackList = new ArrayList<String>();
    final ArrayList<String> hostList = new ArrayList<String>();
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 2; j++) {
        rackList.add("/datacenter0/rack" + i);
        hostList.add("host" + i + j);
      }
    }

    for (int i = 5; i < 10; i++) {
      for (int j = 0; j < 2; j++) {
        rackList.add("/datacenter1/rack" + i);
        hostList.add("host" + i + j);
      }
    }

    rackList.add("/datacenter2/rack0");
    hostList.add("host200");

    rackList.add("/datacenter3/rack0");
    hostList.add("host300");

    conf.setClass(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
        BlockPlacementPolicyRackFaultTolerantDataCenter.class,
        BlockPlacementPolicy.class);
    conf.setBoolean(DFSConfigKeys.DFS_USE_DFS_NETWORK_TOPOLOGY_KEY, true);
    conf.setClass(DFSConfigKeys.DFS_NET_TOPOLOGY_IMPL_KEY,
        DFSNetworkTopologyWithDataCenter.class, DFSNetworkTopology.class);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE / 2);
    conf.set(DFSConfigKeys.DFS_NAMENODE_BLOCK_PLACEMENT_POLICY_WITH_DATA_CENTER_FALLBACK_DC_KEY,
        DEFAULT_DC);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY, 3);
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(hostList.size())
        .racks(rackList.toArray(new String[rackList.size()]))
        .hosts(hostList.toArray(new String[hostList.size()]))
        .build();
    cluster.waitActive();
    nameNodeRpc = cluster.getNameNodeRpc();
    namesystem = cluster.getNamesystem();
    perm = new PermissionStatus("TestBlockPlacementPolicyRackFaultTolerantDataCenter",
        null, FsPermission.getDefault());
  }

  @After
  public void teardown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  private DatanodeInfo[] verifyAddBlock(String clientMachine, int expectedReplicas,
      short replication, boolean failed) {
    final String src = "/test-datacenter" + System.nanoTime();
    try {
      HdfsFileStatus fileStatus = namesystem.startFile(src, perm, clientMachine, clientMachine,
          EnumSet.of(CreateFlag.CREATE), true, replication,
          DEFAULT_BLOCK_SIZE, null, null, null,
          false);
      LocatedBlock locatedBlock = nameNodeRpc.addBlock(src, clientMachine, null,
          null, fileStatus.getFileId(), null, null);
      DatanodeInfo[] locations = locatedBlock.getLocations();
      if (failed) {
        fail("Adding block should fail.");
      } else {
        assertEquals(replication, locations.length);
        nameNodeRpc.abandonBlock(locatedBlock.getBlock(), fileStatus.getFileId(),
            src, clientMachine);
        return locations;
      }
    } catch (IOException e) {
      if (failed) {
        String errMsg = "File " + src + " could only be written to " + expectedReplicas;
        assertTrue(e.getMessage().contains(errMsg));
      } else {
        fail("Adding block should not fail.");
      }
    }
    return null;
  }

  private void verifyDatacenter(String clientDataCenter, String datanodeRack) {
    assertEquals(clientDataCenter, DFSNetworkTopologyWithDataCenter.getDataCenter(datanodeRack));
  }

  private void logLocations(DatanodeInfo[] locations) {
    for (int i = 0; i < locations.length; i++) {
      LOG.info("Datanode " + i +
          ": (loc=" + locations[i].getNetworkLocation() +
          ", host=" + locations[i].getHostName() +
          ")");
    }
  }

  /**
   * Test the normal placement of data blocks within the specified data center.
   */
  @Test
  public void testDatacenterNormalPlacement() throws Exception {
    testDatacenterNormalPlacement("host00", "/datacenter0/rack0", (short) 9);
    testDatacenterNormalPlacement("host50", "/datacenter1/rack5", (short) 3);
  }

  private void testDatacenterNormalPlacement(String clientMachine, String clientRack,
      short replication) {
    String dataCenter = DFSNetworkTopologyWithDataCenter.getDataCenter(clientRack);
    for (int i = 0; i < times; i++) {
      Set<String> racks = Sets.newHashSet();
      DatanodeInfo[] locations = verifyAddBlock(clientMachine, replication, replication,
          false);
      assert locations != null;
      logLocations(locations);
      for (DatanodeInfo datanodeInfo : locations) {
        racks.add(datanodeInfo.getNetworkLocation());
        verifyDatacenter(dataCenter, datanodeInfo.getNetworkLocation());
      }

      // Verify that the number of rack matches the specified replication factor.
      assertEquals(replication, racks.size());
    }
  }

  /**
   * Test data block placement with fallback to the default data center.
   */
  @Test
  public void testDatacenterFallBackDefaultDCPlacement() {
    // Set up client machine in datacenter9.
    String clientMachine = "client.foo.com";
    String clientRack = "/datacenter9/rack0";
    StaticMapping.addNodeToRack(clientMachine, clientRack);
    for (int i = 0; i < times; i++) {
      Set<String> racks = Sets.newHashSet();
      DatanodeInfo[] locations = verifyAddBlock(clientMachine, (short) 9, (short) 9,
          false);
      assert locations != null;
      logLocations(locations);
      // Verify that the data block is placed in the default data center.
      for (DatanodeInfo datanodeInfo : locations) {
        racks.add(datanodeInfo.getNetworkLocation());
        verifyDatacenter("/" + DEFAULT_DC, datanodeInfo.getNetworkLocation());
      }
    }
  }

  /**
   * Test block placement fallback to default data center failed.
   */
  @Test
  public void testNotEnoughDNsInDefaultDC() {
    // Set default fallback DC to datacenter3, which has only 1 DN.
    BlockPlacementPolicyRackFaultTolerantDataCenter policy =
        (BlockPlacementPolicyRackFaultTolerantDataCenter) namesystem.getBlockManager()
            .getBlockPlacementPolicy();
    policy.setDefaultDC("datacenter3");

    // Client from dc8 tries to add block. Fall back to dc3, only 1 replica found.
    String clientMachine = "client.foo.com";
    String clientRack = "/datacenter8/rack0";
    StaticMapping.addNodeToRack(clientMachine, clientRack);
    verifyAddBlock(clientMachine, 1, (short) 9, true);
    // Client from dc2 tries to add block. No fall back because 1 replica found on dc2.
    clientRack = "/datacenter2/rack0";
    StaticMapping.addNodeToRack(clientMachine, clientRack);
    verifyAddBlock(clientMachine, 1, (short) 9, true);
  }


  /**
   * Override UT the
   * {@link org.apache.hadoop.hdfs.server.namenode.TestBlockPlacementPolicyRackFaultTolerant}
   * method, which includes both normal and special cases base on datacenter.
   */
  @Test
  public void testChooseTarget() throws Exception {
    doTestChooseTargetNormalCase();
    doTestChooseTargetSpecialCase();
  }

  private void doTestChooseTargetNormalCase() throws Exception {
    String clientMachine = "client.foo.com";
    short[][] testSuite = {
        {3, 2}, {3, 7}, {3, 8}, {3, 10}, {9, 1}, {10, 1}, {10, 6}, {11, 6},
        {11, 9}
    };
    // Test 5 files
    int fileCount = 0;
    for (int i = 0; i < 5; i++) {
      for (short[] testCase : testSuite) {
        short replication = testCase[0];
        short additionalReplication = testCase[1];
        String src = "/testfile" + (fileCount++);
        // Create the file with client machine
        HdfsFileStatus fileStatus = namesystem.startFile(src, perm,
            clientMachine, clientMachine, EnumSet.of(CreateFlag.CREATE), true,
            replication, DEFAULT_BLOCK_SIZE, null, null,
            null, false);

        //test chooseTarget for new file
        LocatedBlock locatedBlock = nameNodeRpc.addBlock(src, clientMachine,
            null, null, fileStatus.getFileId(), null,
            null);
        doTestLocatedBlock(replication, locatedBlock);

        //test chooseTarget for existing file.
        LocatedBlock additionalLocatedBlock =
            nameNodeRpc.getAdditionalDatanode(src, fileStatus.getFileId(),
                locatedBlock.getBlock(), locatedBlock.getLocations(),
                locatedBlock.getStorageIDs(), DatanodeInfo.EMPTY_ARRAY,
                additionalReplication, clientMachine);
        doTestLocatedBlock(replication + additionalReplication, additionalLocatedBlock);
      }
    }
  }

  /**
   * Test more randomly. So it covers some special cases.
   * Like when some racks already have 2 replicas, while some racks have none,
   * we should choose the racks that have none.
   */
  private void doTestChooseTargetSpecialCase() throws Exception {
    String clientMachine = "client.foo.com";
    // Test 5 files
    String src = "/testfile_1_";
    // Create the file with client machine
    HdfsFileStatus fileStatus = namesystem.startFile(src, perm,
        clientMachine, clientMachine, EnumSet.of(CreateFlag.CREATE), true,
        (short) 20, DEFAULT_BLOCK_SIZE, null, null,
        null, false);

    //test chooseTarget for new file
    LocatedBlock locatedBlock = nameNodeRpc.addBlock(src, clientMachine,
        null, null, fileStatus.getFileId(), null, null);
    doTestLocatedBlock(20, locatedBlock);

    DatanodeInfo[] locs = locatedBlock.getLocations();
    String[] storageIDs = locatedBlock.getStorageIDs();

    for (int time = 0; time < 5; time++) {
      shuffle(locs, storageIDs);
      for (int i = 1; i < locs.length; i++) {
        DatanodeInfo[] partLocs = new DatanodeInfo[i];
        String[] partStorageIDs = new String[i];
        System.arraycopy(locs, 0, partLocs, 0, i);
        System.arraycopy(storageIDs, 0, partStorageIDs, 0, i);
        for (int j = 1; j < 20 - i; j++) {
          LocatedBlock additionalLocatedBlock =
              nameNodeRpc.getAdditionalDatanode(src, fileStatus.getFileId(),
                  locatedBlock.getBlock(), partLocs,
                  partStorageIDs, DatanodeInfo.EMPTY_ARRAY,
                  j, clientMachine);
          doTestLocatedBlock(i + j, additionalLocatedBlock);
        }
      }
    }
  }

  /**
   * Override UT the
   * {@link org.apache.hadoop.hdfs.server.namenode.TestBlockPlacementPolicyRackFaultTolerant}
   * method, verify decommission a dn which is an only node in its rack base on datacenter.
   */
  @Test
  public void testPlacementWithOnlyOneNodeInRackDecommission() throws Exception {
    Configuration conf = new HdfsConfiguration();
    final String[] racks = {
        "/datacenter0/rack0", "/datacenter0/rack1", "/datacenter0/rack1",
        "/datacenter0/rack2", "/datacenter0/rack4", "/datacenter0/rack5",
        "/datacenter0/rack5", "/datacenter1/rack0", "/datacenter1/rack0",
        "/datacenter2/rack0", "/datacenter2/rack0", "/datacenter2/rack0" };
    final String[] hosts = {
        "host0", "host1", "host2", "host3", "host4", "host5", "host6",
        "host7", "host8", "host9", "host10", "host11" };

    // enables DFSNetworkTopology
    conf.setClass(DFSConfigKeys.DFS_BLOCK_PLACEMENT_EC_CLASSNAME_KEY,
        BlockPlacementPolicyRackFaultTolerantDataCenter.class,
        BlockPlacementPolicy.class);
    conf.setBoolean(DFSConfigKeys.DFS_USE_DFS_NETWORK_TOPOLOGY_KEY, true);
    conf.setClass(DFSConfigKeys.DFS_NET_TOPOLOGY_IMPL_KEY,
        DFSNetworkTopologyWithDataCenter.class, DFSNetworkTopology.class);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE / 2);
    conf.set(DFSConfigKeys.DFS_NAMENODE_BLOCK_PLACEMENT_POLICY_WITH_DATA_CENTER_FALLBACK_DC_KEY,
        DEFAULT_DC);

    if (cluster != null) {
      cluster.shutdown();
    }
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(12).racks(racks).hosts(hosts).build();
    cluster.waitActive();
    nameNodeRpc = cluster.getNameNodeRpc();
    namesystem = cluster.getNamesystem();
    DistributedFileSystem fs = cluster.getFileSystem();
    fs.enableErasureCodingPolicy("RS-3-2-1024k");
    fs.setErasureCodingPolicy(new Path("/"), "RS-3-2-1024k");

    final BlockManager bm = cluster.getNamesystem().getBlockManager();
    final DatanodeManager dm = bm.getDatanodeManager();
    assertTrue(dm.getNetworkTopology() instanceof DFSNetworkTopology);

    String clientMachine = "host4";
    String clientRack = "/datacenter0/rack4";
    String src = "/test";

    final DatanodeManager dnm = namesystem.getBlockManager().getDatanodeManager();
    DatanodeDescriptor dnd4 = dnm.getDatanode(cluster.getDataNodes().get(4).getDatanodeId());
    assertEquals(dnd4.getNetworkLocation(), clientRack);
    dnm.getDatanodeAdminManager().startDecommission(dnd4);
    short replication = 5;
    short additionalReplication = 1;

    try {
      // Create the file with client machine
      HdfsFileStatus fileStatus = namesystem.startFile(src, perm,
          clientMachine, clientMachine, EnumSet.of(CreateFlag.CREATE), true,
          replication, DEFAULT_BLOCK_SIZE * 1024 * 10, null,
          null, null, false);

      //test chooseTarget for new file
      LocatedBlock locatedBlock = nameNodeRpc.addBlock(src, clientMachine,
          null, null, fileStatus.getFileId(), null,
          null);
      HashMap<String, Integer> racksCount = new HashMap<String, Integer>();
      doTestLocatedBlockRacks(racksCount, replication, 4, locatedBlock);

      //test chooseTarget for existing file.
      LocatedBlock additionalLocatedBlock =
          nameNodeRpc.getAdditionalDatanode(src, fileStatus.getFileId(),
              locatedBlock.getBlock(), locatedBlock.getLocations(),
              locatedBlock.getStorageIDs(), DatanodeInfo.EMPTY_ARRAY,
              additionalReplication, clientMachine);

      racksCount.clear();
      doTestLocatedBlockRacks(racksCount, additionalReplication + replication,
          4, additionalLocatedBlock);
      assertEquals(racksCount.get("/datacenter0/rack5"), (Integer)2);
      assertEquals(racksCount.get("/datacenter0/rack1"), (Integer)2);
      assertEquals(4, bm.getDatanodeManager().
          getNetworkTopology().getNumOfNonEmptyRacks("/" + DEFAULT_DC));
    } finally {
      dnm.getDatanodeAdminManager().stopDecommission(dnd4);
    }

    //test if decommission succeeded
    DatanodeDescriptor dnd3 = dnm.getDatanode(cluster.getDataNodes().get(3).getDatanodeId());
    cluster.getNamesystem().writeLock();
    try {
      dm.getDatanodeAdminManager().startDecommission(dnd3);
    } finally {
      cluster.getNamesystem().writeUnlock();
    }

    // make sure the decommission finishes and the block in on 3 racks
    GenericTestUtils.waitFor(dnd3::isDecommissioned, 1000, 10 * 1000);

    LocatedBlocks locatedBlocks =
        cluster.getFileSystem().getClient().getLocatedBlocks(
            src, 0, DEFAULT_BLOCK_SIZE);
    assertEquals(4, bm.getDatanodeManager().
        getNetworkTopology().getNumOfNonEmptyRacks("/" + DEFAULT_DC));
    for (LocatedBlock block : locatedBlocks.getLocatedBlocks()) {
      BlockPlacementStatus status = bm.getStripedBlockPlacementPolicy()
          .verifyBlockPlacement(block.getLocations(), 5);
      Assert.assertTrue(status.isPlacementPolicySatisfied());
    }
  }

  private void shuffle(DatanodeInfo[] locs, String[] storageIDs) {
    int length = locs.length;
    Object[][] pairs = new Object[length][];
    for (int i = 0; i < length; i++) {
      pairs[i] = new Object[]{locs[i], storageIDs[i]};
    }
    Collections.shuffle(Arrays.asList(pairs));
    for (int i = 0; i < length; i++) {
      locs[i] = (DatanodeInfo) pairs[i][0];
      storageIDs[i] = (String) pairs[i][1];
    }
  }

  private void doTestLocatedBlock(int replication, LocatedBlock locatedBlock) {
    logLocations(locatedBlock.getLocations());
    assertEquals(replication, locatedBlock.getLocations().length);

    HashMap<String, Integer> racksCount = new HashMap<String, Integer>();
    for (DatanodeInfo node :
        locatedBlock.getLocations()) {
      verifyDatacenter("/" + DEFAULT_DC, node.getNetworkLocation());
      addToRacksCount(node.getNetworkLocation(), racksCount);
    }

    int minCount = Integer.MAX_VALUE;
    int maxCount = Integer.MIN_VALUE;
    for (Integer rackCount : racksCount.values()) {
      minCount = Math.min(minCount, rackCount);
      maxCount = Math.max(maxCount, rackCount);
    }
    assertTrue(maxCount - minCount <= 1);
  }

  private void doTestLocatedBlockRacks(HashMap<String, Integer> racksCount, int replication,
      int validracknum, LocatedBlock locatedBlock) {
    assertEquals(replication, locatedBlock.getLocations().length);
    logLocations(locatedBlock.getLocations());
    for (DatanodeInfo node :
        locatedBlock.getLocations()) {
      verifyDatacenter("/" + DEFAULT_DC, node.getNetworkLocation());
      addToRacksCount(node.getNetworkLocation(), racksCount);
    }
    assertEquals(validracknum, racksCount.size());
  }

  private void addToRacksCount(String rack, HashMap<String, Integer> racksCount) {
    Integer count = racksCount.get(rack);
    if (count == null) {
      racksCount.put(rack, 1);
    } else {
      racksCount.put(rack, count + 1);
    }
  }
}
