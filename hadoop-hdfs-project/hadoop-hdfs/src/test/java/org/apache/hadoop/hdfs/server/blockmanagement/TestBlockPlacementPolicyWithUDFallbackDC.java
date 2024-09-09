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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.net.DFSNetworkTopology;
import org.apache.hadoop.hdfs.net.DFSNetworkTopologyWithDataCenter;
import org.apache.hadoop.hdfs.net.NetworkTopologyUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeAdminProperties;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.util.HostsFileWriter;
import org.apache.hadoop.net.StaticMapping;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestBlockPlacementPolicyWithUDFallbackDC {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestBlockPlacementPolicyWithUDFallbackDC.class);
  private static final short REPLICATION_FACTOR = (short) 3;
  private static final int DEFAULT_BLOCK_SIZE = 1024;
  private MiniDFSCluster cluster = null;
  private NamenodeProtocols nameNodeRpc = null;
  private FSNamesystem namesystem = null;
  private PermissionStatus perm = null;
  private final int times = 10;

  final String[] racks = {
      "/datacenter0/rack0", "/datacenter0/rack1", "/datacenter0/rack2"};
  final String[] hosts = {
      "host0", "host1", "host2"
  };
  final String[] upgradeDomains = {
      "/datacenter0/ud1", "/datacenter0/ud2", "/datacenter0/ud3"
  };

  private final HostsFileWriter hostsFileWriter = new HostsFileWriter();

  @Before
  public void setup() throws IOException {
    StaticMapping.resetMap();
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.set(DFSConfigKeys.DFS_NAMENODE_BLOCK_PLACEMENT_POLICY_WITH_DATA_CENTER_FALLBACK_DC_KEY,
        "/datacenter0");
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE / 2);
    conf.setClass(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
        BlockPlacementPolicyWithUDFallbackDC.class,
        BlockPlacementPolicy.class);
    conf.setBoolean(DFSConfigKeys.DFS_USE_DFS_NETWORK_TOPOLOGY_KEY, true);
    conf.setClass(DFSConfigKeys.DFS_NET_TOPOLOGY_IMPL_KEY,
        DFSNetworkTopologyWithDataCenter.class,
        DFSNetworkTopology.class);
    conf.setBoolean(CommonConfigurationKeys.IGNORE_SDI_AUTHENTICATE_KEY, true);
    conf.setClass(DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY,
        CombinedHostFileManager.class, HostConfigManager.class);
    hostsFileWriter.initialize(conf, "temp/upgradedomainpolicy2");

    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3)
        .racks(racks)
        .hosts(hosts)
        .build();

    cluster.waitActive();
    nameNodeRpc = cluster.getNameNodeRpc();
    namesystem = cluster.getNamesystem();
    perm = new PermissionStatus("TestBlockPlacementPolicyWithUDFallbackDC",
        null, FsPermission.getDefault());
    refreshDatanodeAdminProperties(conf);
  }

  private Map<String, Pair<String, Integer>> getDataNodeHost2Ip() {
    Map<String, Pair<String, Integer>> map = new HashMap<>();
    for (DataNode dataNode : cluster.getDataNodes()) {
      map.put(dataNode.getDatanodeHostname(),
          Pair.of(dataNode.getDatanodeId().getIpAddr(), dataNode.getDatanodeId().getXferPort()));
    }
    return map;
  }

  private void refreshDatanodeAdminProperties(Configuration conf) throws IOException {
    DatanodeAdminProperties[] datanodes = new DatanodeAdminProperties[hosts.length];
    Map<String, Pair<String, Integer>> map = getDataNodeHost2Ip();
    for (int i = 0; i < hosts.length; i++) {
      datanodes[i] = new DatanodeAdminProperties();
      Pair<String, Integer> pair = map.get(hosts[i]);
      datanodes[i].setHostName(pair.getLeft());
      datanodes[i].setPort(pair.getRight());
      datanodes[i].setUpgradeDomain(upgradeDomains[i]);
    }
    hostsFileWriter.initIncludeHosts(datanodes);
    namesystem.getBlockManager().getDatanodeManager().refreshNodes(conf);
  }


  @After
  public void teardown() throws IOException {
    hostsFileWriter.cleanup();
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
    String filePath = "/test-datacenter" + System.nanoTime();
    // Create the file with client machine
    HdfsFileStatus fileStatus = namesystem.startFile(filePath, perm,
        clientMachine, clientMachine, EnumSet.of(CreateFlag.CREATE), true,
        REPLICATION_FACTOR, DEFAULT_BLOCK_SIZE, null, null, null, false);
    LocatedBlock locatedBlock = nameNodeRpc.addBlock(filePath, clientMachine,
        null, null, fileStatus.getFileId(), null, null);
    DatanodeInfo[] locations = locatedBlock.getLocations();
    nameNodeRpc.abandonBlock(locatedBlock.getBlock(), fileStatus.getFileId(),
        filePath, clientMachine);
    return locations;
  }

  private Set<String> getDomains(DatanodeInfo... dns) {
    Set<String> domains = new HashSet<>();
    for (DatanodeInfo dn : dns) {
      domains.add(dn.getUpgradeDomain());
    }
    return domains;
  }

  private boolean isSameDC(String clientRack, DatanodeInfo datanodeInfo) {
    return DFSNetworkTopologyWithDataCenter.getDataCenter(clientRack)
        .equals(NetworkTopologyUtil.getDataCenter(datanodeInfo));
  }

  private void logLocations(DatanodeInfo[] locations) {
    for (int i = 0; i < locations.length; i++) {
      LOG.info("Datanode {}:(loc={}, host={}, ud={})", i, locations[i].getNetworkLocation(),
          locations[i].getHostName(), locations[i].getUpgradeDomain());
    }
  }

  @Test
  public void testNormalDCWithUD() throws Exception {
    String clientMachine = "client.foo.com";
    String clientRack = "/datacenter0/rack0";
    StaticMapping.addNodeToRack(clientMachine, clientRack);
    for (int i = 0; i < times; i++) {
      LOG.info("Start round {}....", i);
      DatanodeInfo[] locations = getLocations(clientMachine);
      logLocations(locations);

      assertEquals(3, locations.length);
      assertTrue(isSameDC(clientRack, locations[0]));
      assertTrue(isSameDC(clientRack, locations[1]));
      assertTrue(isSameDC(clientRack, locations[2]));
      assertEquals(3, getDomains(locations).size());
    }
  }

  @Test
  public void testFallbackCase() throws Exception {
    String clientMachine = "client.foo.com";
    String clientRack = "/datacenter1/rack0";
    StaticMapping.addNodeToRack(clientMachine, clientRack);
    for (int i = 0; i < times; i++) {
      LOG.info("Start round {}....", i);
      DatanodeInfo[] locations = getLocations(clientMachine);
      logLocations(locations);

      // Can only choose 2 dataNodes with different ud
      assertEquals(3, locations.length);
      assertTrue(isSameDC("/datacenter0/rack0", locations[0]));
      assertTrue(isSameDC("/datacenter0/rack0", locations[1]));
      assertTrue(isSameDC("/datacenter0/rack0", locations[2]));
      assertEquals(3, getDomains(locations).size());
    }
  }
}
