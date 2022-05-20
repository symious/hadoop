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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.net.DFSNetworkTopology;
import org.apache.hadoop.hdfs.net.DFSNetworkTopologyWithDataCenter;
import org.apache.hadoop.hdfs.net.NetworkTopologyUtil;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.server.zoneservice.ReplicationRule;
import org.apache.hadoop.net.StaticMapping;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class TestBlockPlacementPolicyWithDataCenter {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestBlockPlacementPolicyWithDataCenter.class);
  private static final short REPLICATION_FACTOR = (short) 3;
  private static final int DEFAULT_BLOCK_SIZE = 1024;
  private MiniDFSCluster cluster = null;
  private NamenodeProtocols nameNodeRpc = null;
  private FSNamesystem namesystem = null;
  private PermissionStatus perm = null;
  private final int times = 10;

  @Before
  public void setup() throws IOException {
    StaticMapping.resetMap();
    Configuration conf = new HdfsConfiguration();
    final String[] racks = {
        "/datacenter0/rack0", "/datacenter0/rack0", "/datacenter0/rack1",
        "/datacenter0/rack1", "/datacenter1/rack0", "/datacenter1/rack1",
        "/datacenter0/rack0", "/datacenter1/rack0", "/datacenter1/rack0",
        "/datacenter2/rack0", "/datacenter2/rack0", "/datacenter2/rack0"
    };
    final String[] hosts = {
        "host0", "host1", "host2",
        "host3", "host4", "host5",
        "host6", "host7", "host8",
        "host9", "host10", "host11"
    };

    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE / 2);
    conf.setClass(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
        BlockPlacementPolicyWithDataCenter.class,
        BlockPlacementPolicy.class);
    conf.setBoolean(DFSConfigKeys.DFS_USE_DFS_NETWORK_TOPOLOGY_KEY, true);
    conf.setClass(DFSConfigKeys.DFS_NET_TOPOLOGY_IMPL_KEY,
        DFSNetworkTopologyWithDataCenter.class,
        DFSNetworkTopology.class);
    conf.setBoolean(CommonConfigurationKeys.IGNORE_SDI_AUTHENTICATE_KEY,
        true);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(12).racks(racks)
        .hosts(hosts).build();
    cluster.waitActive();
    nameNodeRpc = cluster.getNameNodeRpc();
    namesystem = cluster.getNamesystem();
    perm = new PermissionStatus("TestBlockPlacementPolicyWithDataCenter",
        null, FsPermission.getDefault());
  }

  @After
  public void teardown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * Get locations of the first block for a test file.
   * @param clientMachine the client's machine
   * @return locations
   */
  private DatanodeInfo[] getLocations(String clientMachine) throws IOException {
    String src = "/test-datacenter" + System.nanoTime();
    // Create the file with client machine
    HdfsFileStatus fileStatus = namesystem.startFile(src, perm,
        clientMachine, clientMachine, EnumSet.of(CreateFlag.CREATE), true,
        REPLICATION_FACTOR, DEFAULT_BLOCK_SIZE, null, null,
        null, false);
    LocatedBlock locatedBlock = nameNodeRpc.addBlock(src, clientMachine,
        null, null, fileStatus.getFileId(), null, null);
    DatanodeInfo[] locations = locatedBlock.getLocations();
    assertEquals("Block should be allocated sufficient locations",
        REPLICATION_FACTOR, locations.length);
    nameNodeRpc.abandonBlock(locatedBlock.getBlock(), fileStatus.getFileId(),
        src, clientMachine);
    return locations;
  }

  private boolean isNodeLocal(String clientHost,
      String clientRack, DatanodeInfo datanodeInfo) {
    return (clientHost.equals(datanodeInfo.getHostName()) &&
        clientRack.equals(datanodeInfo.getNetworkLocation()));
  }

  private boolean isRackLocal(String clientHost,
      String clientRack, DatanodeInfo datanodeInfo) {
    return (!clientHost.equals(datanodeInfo.getHostName()) &&
        clientRack.equals(datanodeInfo.getNetworkLocation()));
  }

  private boolean isDatacenterLocal(String clientHost,
                                    String clientRack,
                                    DatanodeInfo datanodeInfo) {
    return (!clientHost.equals(datanodeInfo.getHostName()) &&
        !clientRack.equals(datanodeInfo.getNetworkLocation()) &&
        DFSNetworkTopologyWithDataCenter.getDataCenter(clientRack).equals(
            NetworkTopologyUtil.getDataCenter(datanodeInfo)
        ));
  }

  private void logLocations(DatanodeInfo[] locations) {
    for (int i=0; i < locations.length; i++) {
      LOG.info("Datanode " + i +
          ": (loc=" + locations[i].getNetworkLocation() +
          ", host=" + locations[i].getHostName() +
          ")");
    }
  }

  @Test
  public void testNodeLocalPlacement() throws Exception {
    String clientMachine = "host0";
    String clientRack = "/datacenter0/rack0";
    for (int i = 0; i < times; i++) {
      LOG.info("Start round " + i + " ...");
      DatanodeInfo[] locations = getLocations(clientMachine);
      logLocations(locations);
      // As the logic of "FSDirWriteFileOp.chooseTargetForNewBlock" and
      // "Host2NodesMap.getDatanodeByHost", HDFS will treat the client as a
      // datanode if clientMachine can match an IP address of a datanode.
      // But if we use "127.0.0.1", HDFS will just return a random datanode
      // as all datanode are sharing "127.0.0.1" during unit testing.
      // When using "host0", the client will be treated a a general node
      // instead of a datanode, and "chooseLocalStorage" will fall
      // back to "chooseLocalRack".
      assertTrue("1st datanode should be node local or rack-local",
          isNodeLocal(clientMachine, clientRack, locations[0]) ||
              isRackLocal(clientMachine, clientRack, locations[0]));
      assertTrue("2nd datanode should be datacenter-local",
          isDatacenterLocal(clientMachine, clientRack, locations[1]));
      assertTrue("3rd datanode should be rack-local with 2nd datanode",
          isRackLocal(locations[1].getHostName(),
              locations[1].getNetworkLocation(), locations[2]));
    }
  }

  @Test
  public void testRackLocalPlacement() throws Exception {
    String clientMachine = "client.foo.com";
    String clientRack = "/datacenter0/rack0";
    StaticMapping.addNodeToRack(clientMachine, clientRack);
    for (int i = 0; i < times; i++) {
      LOG.info("Start round " + i + " ...");
      DatanodeInfo[] locations = getLocations(clientMachine);
      logLocations(locations);
      assertTrue("1st datanode should be rack-local",
          isRackLocal(clientMachine, clientRack, locations[0]));
      assertTrue("2nd datanode should be datacenter-local",
          isDatacenterLocal(clientMachine, clientRack, locations[1]));
      assertTrue("3rd datanode should be rack-local with 2nd datanode",
          isRackLocal(locations[1].getHostName(),
              locations[1].getNetworkLocation(), locations[2]));
    }

  }

  @Test
  public void testDatacenterLocalPlacement() throws Exception {
    String clientMachine = "client.foo.com";
    String clientRack = "/datacenter0/rack2";
    StaticMapping.addNodeToRack(clientMachine, clientRack);
    for (int i = 0; i < times; i++) {
      LOG.info("Start round " + i + " ...");
      DatanodeInfo[] locations = getLocations(clientMachine);
      logLocations(locations);
      assertTrue("1st datanode should be datacenter-local",
          isDatacenterLocal(clientMachine, clientRack, locations[0]));
      assertTrue("2nd datanode should be datacenter-local with 1st datanode",
          isDatacenterLocal(locations[0].getHostName(),
              locations[0].getNetworkLocation(), locations[1]));
      assertTrue("3rd datanode should be rack-local with 2nd datanode",
          isRackLocal(locations[1].getHostName(),
              locations[1].getNetworkLocation(), locations[2]));
    }
  }
  @Test
  public void testDatacenterOffPlacement() {
    String clientMachine = "client.foo.com";
    String clientRack = "/datacenter9/rack0";
    StaticMapping.addNodeToRack(clientMachine, clientRack);
    for (int i = 0; i < times; i++) {
      LOG.info("Start round " + i + " ...");
      try {
        // We do not support datacenter-off placement
        getLocations(clientMachine);
        fail("Expected IOException");
      } catch (IOException e) {
        assertTrue(e.getMessage().contains(
            "could only be written to 0 of the 1 minReplication nodes"));
      }
    }
  }

  @Test
  public void testChooseReplicasToDelete() {
    Collection<DatanodeStorageInfo> nonExcess = new ArrayList<>();
    ReplicationRule rule = ReplicationRule
        .parseFromString("/datacenter0:2,/datacenter1:1");
    BlockPlacementPolicyWithDataCenter policy =
        (BlockPlacementPolicyWithDataCenter) namesystem.getBlockManager()
            .getBlockPlacementPolicy();
    Set<DatanodeDescriptor> datanodes=
        namesystem.getBlockManager().getDatanodeManager().getDatanodes();

    // Sort datanodes by rack, and add the DatanodeStorageInfo to map.
    Map<String, List<DatanodeStorageInfo>> dcMap = getDcMapFromDatanodes(datanodes);

    List<DatanodeStorageInfo> excessReplicas;
    BlockStoragePolicySuite POLICY_SUITE = BlockStoragePolicySuite
        .createDefaultSuite();
    BlockStoragePolicy storagePolicy = POLICY_SUITE.getDefaultPolicy();

    // Add the DatanodeStorageInfo of dc0 & dc1 to nonExcess
    nonExcess.add(dcMap.get("/datacenter0/rack0").get(0));
    nonExcess.add(dcMap.get("/datacenter0/rack0").get(1));
    nonExcess.add(dcMap.get("/datacenter0/rack0").get(2));
    nonExcess.add(dcMap.get("/datacenter1/rack0").get(0));
    nonExcess.add(dcMap.get("/datacenter1/rack0").get(1));

    // Use delete hint case.
    DatanodeDescriptor delHintNode = dcMap.get("/datacenter0/rack0")
        .get(0).getDatanodeDescriptor();
    List<StorageType> excessTypes = storagePolicy.chooseExcess((short) 3,
        DatanodeStorageInfo.toStorageTypes(nonExcess));
    excessReplicas = policy.chooseReplicasToDelete(nonExcess,
        nonExcess, 3, rule, excessTypes,
        dcMap.get("/datacenter1/rack0").get(1).getDatanodeDescriptor(), delHintNode);
    assertEquals(2, excessReplicas.size());
    assertTrue(excessReplicas.contains(dcMap.get("/datacenter0/rack0").get(0)));

    // Excess type deletion
    DatanodeStorageInfo excessStorage = DFSTestUtil.createDatanodeStorageInfo(
        "Storage-excess-ID", "localhost", delHintNode.getNetworkLocation(),
        "foo.com", StorageType.ARCHIVE, null);
    nonExcess.add(excessStorage);
    excessTypes = storagePolicy.chooseExcess((short) 3,
        DatanodeStorageInfo.toStorageTypes(nonExcess));
    excessReplicas = policy.chooseReplicasToDelete(nonExcess,
        nonExcess, 3, rule, excessTypes,
        dcMap.get("/datacenter1/rack0").get(1).getDatanodeDescriptor(), null);
    assertTrue(excessReplicas.contains(excessStorage));
    assertEquals(3, excessReplicas.size());

    // The Rule includes parts of all the given replicas.
    rule = ReplicationRule
        .parseFromString("/datacenter0:2,/datacenter2:1");
    nonExcess.clear();
    nonExcess.add(dcMap.get("/datacenter0/rack0").get(0));
    nonExcess.add(dcMap.get("/datacenter0/rack0").get(1));
    nonExcess.add(dcMap.get("/datacenter0/rack0").get(2));
    nonExcess.add(dcMap.get("/datacenter0/rack1").get(0));
    nonExcess.add(dcMap.get("/datacenter1/rack0").get(0));
    excessTypes = storagePolicy.chooseExcess((short) 3,
        DatanodeStorageInfo.toStorageTypes(nonExcess));
    excessReplicas = policy.chooseReplicasToDelete(nonExcess,
        nonExcess, 3, rule, excessTypes,
        dcMap.get("/datacenter1/rack0").get(1).getDatanodeDescriptor(), null);
    assertEquals(2, excessReplicas.size());

    // delNodeHint is not in the rule.
    rule = ReplicationRule
        .parseFromString("/datacenter0:1,/datacenter1:2");
    nonExcess.clear();
    nonExcess.add(dcMap.get("/datacenter0/rack0").get(0));
    nonExcess.add(dcMap.get("/datacenter1/rack0").get(0));
    nonExcess.add(dcMap.get("/datacenter2/rack0").get(0));
    nonExcess.add(dcMap.get("/datacenter2/rack0").get(1));
    delHintNode = dcMap.get("/datacenter2/rack0").get(1).getDatanodeDescriptor();
    excessReplicas = policy.chooseReplicasToDelete(
        nonExcess, nonExcess, 3, rule, excessTypes,
        dcMap.get("/datacenter1/rack0").get(0).getDatanodeDescriptor(), delHintNode);
    assertEquals(1, excessReplicas.size());
    // delHintNode is in excessReplicas.
    assertEquals(delHintNode, excessReplicas.get(0).getDatanodeDescriptor());
  }

  @Test
  public void testChooseTarget() {
    BlockPlacementPolicyWithDataCenter policy =
        (BlockPlacementPolicyWithDataCenter) namesystem.getBlockManager()
            .getBlockPlacementPolicy();
    BlockStoragePolicy storagePolicy = BlockStoragePolicySuite
        .createDefaultSuite().getDefaultPolicy();

    // Sort datanodes by rack, and add the DatanodeStorageInfo to map.
    Set<DatanodeDescriptor> datanodes=
        namesystem.getBlockManager().getDatanodeManager().getDatanodes();
    Map<String, List<DatanodeStorageInfo>> dcMap = getDcMapFromDatanodes(datanodes);

    // replicate to the datacenter without any replicas first
    ReplicationRule rule = ReplicationRule
        .parseFromString("/datacenter0:2,/datacenter1:2");
    List<DatanodeStorageInfo> existNodes = new ArrayList<>();
    existNodes.add(dcMap.get("/datacenter0/rack0").get(0));
    DatanodeStorageInfo[] results = policy.chooseTarget(null, 3, rule, null,
        existNodes, false, null, DEFAULT_BLOCK_SIZE, storagePolicy, null);
    assertEquals(2, results.length);
    for (DatanodeStorageInfo info: results) {
      assertEquals("/datacenter1", NetworkTopologyUtil
          .getDataCenter(info.getDatanodeDescriptor()));
    }

    // only replicate to one datacenter one time
    existNodes.clear();
    existNodes.add(dcMap.get("/datacenter0/rack0").get(0));
    existNodes.add(dcMap.get("/datacenter1/rack0").get(0));
    results = policy.chooseTarget(null, 2, rule, null,
        existNodes, false, null, DEFAULT_BLOCK_SIZE, storagePolicy, null);
    assertEquals(1, results.length);
    String dc = NetworkTopologyUtil.getDataCenter(results[0].getDatanodeDescriptor());
    assertTrue(dc.equals("/datacenter0") || dc.equals("/datacenter1"));

    // allocated should not beyond numOfReplicas
    rule = ReplicationRule.parseFromString("/datacenter0:3");
    existNodes.clear();
    existNodes.add(dcMap.get("/datacenter0/rack0").get(0));
    results = policy.chooseTarget(null, 1, rule, null,
        existNodes, false, null, DEFAULT_BLOCK_SIZE, storagePolicy, null);
    assertEquals(1, results.length);
    dc = NetworkTopologyUtil.getDataCenter(results[0].getDatanodeDescriptor());
    assertEquals("/datacenter0", dc);

    // replica=0 case
    rule = ReplicationRule.parseFromString("/datacenter0:0");
    existNodes.clear();
    existNodes.add(dcMap.get("/datacenter1/rack0").get(0));
    results = policy.chooseTarget(null, 1, rule, null,
        existNodes, false, null, DEFAULT_BLOCK_SIZE, storagePolicy, null);
    assertEquals(0, results.length);

    // datacenter does not exist case
    rule = ReplicationRule.parseFromString("/datacenter9:2");
    results = policy.chooseTarget(null, 1, rule, null,
        existNodes, false, null, DEFAULT_BLOCK_SIZE, storagePolicy, null);
    assertEquals(0, results.length);
  }

  private Map<String, List<DatanodeStorageInfo>> getDcMapFromDatanodes(
      Set<DatanodeDescriptor> datanodes) {
    Map<String, List<DatanodeStorageInfo>> dcMap = new HashMap<>();
    for (DatanodeDescriptor dn : datanodes) {
      DatanodeStorageInfo storageInfo =  dn.getStorageInfos()[0];
      String rackName = storageInfo.getDatanodeDescriptor().getNetworkLocation();
      List<DatanodeStorageInfo> storageList =
          dcMap.computeIfAbsent(rackName, k -> new ArrayList<>());
      storageList.add(storageInfo);
    }
    return dcMap;
  }
}