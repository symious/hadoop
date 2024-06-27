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
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeAdminProperties;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.utils.UpgradeDomainUtil;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.util.HostsFileWriter;
import org.apache.hadoop.net.StaticMapping;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestBlockPlacementPolicyWithUpgradeDomainForRackFaultTolerant {

  private static final int DEFAULT_BLOCK_SIZE = 1024;
  private MiniDFSCluster cluster = null;
  private NamenodeProtocols nameNodeRpc = null;
  private FSNamesystem namesystem = null;
  private PermissionStatus perm = null;
  final ArrayList<String> rackList = new ArrayList<String>();
  final ArrayList<String> hostList = new ArrayList<String>();
  final ArrayList<String> upgradeDomainList = new ArrayList<String>();
  private final HostsFileWriter hostsFileWriter = new HostsFileWriter();

  @Before
  public void setup() throws IOException {
    StaticMapping.resetMap();
    final Configuration conf = new HdfsConfiguration();
    int count = 1;
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 2; j++) {
        rackList.add("/rack" + i);
        hostList.add("/host" + i + j);
        if (j == 0) {
          upgradeDomainList.add("/ud-" + count++);
        } else {
          upgradeDomainList.add("/ud-100");
        }
      }
    }
    conf.setClass(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
        BlockPlacementPolicyWithUpgradeDomainForRackFaultTolerant.class,
        BlockPlacementPolicy.class);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE / 2);
    conf.setClass(DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY,
        CombinedHostFileManager.class, HostConfigManager.class);
    hostsFileWriter.initialize(conf, "temp/upgradedomainpolicyforrack");
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(hostList.size())
        .racks(rackList.toArray(new String[0]))
        .hosts(hostList.toArray(new String[0]))
        .build();
    cluster.waitActive();
    nameNodeRpc = cluster.getNameNodeRpc();
    namesystem = cluster.getNamesystem();
    perm = new PermissionStatus("TestBlockPlacementPolicyEC", null,
        FsPermission.getDefault());
    refreshDatanodeAdminProperties(conf);
  }

  Map<String, Pair<String, Integer>> getDataNodeHost2Ip() {
    Map<String, Pair<String, Integer>> map = new HashMap<>();
    for (DataNode dataNode : cluster.getDataNodes()) {
      map.put(dataNode.getDatanodeHostname(),
          Pair.of(dataNode.getDatanodeId().getIpAddr(), dataNode.getDatanodeId().getXferPort()));
    }
    return map;
  }

  private void refreshDatanodeAdminProperties(Configuration conf) throws IOException {
    DatanodeAdminProperties[] datanodes = new DatanodeAdminProperties[hostList.size()];
    Map<String, Pair<String, Integer>> map = getDataNodeHost2Ip();
    for (int i = 0; i < hostList.size(); i++) {
      datanodes[i] = new DatanodeAdminProperties();
      Pair<String, Integer> pair = map.get(hostList.get(i));
      datanodes[i].setHostName(pair.getLeft());
      datanodes[i].setPort(pair.getRight());
      datanodes[i].setUpgradeDomain(upgradeDomainList.get(i));
    }
    hostsFileWriter.initIncludeHosts(datanodes);
    namesystem.getBlockManager().getDatanodeManager().refreshNodes(conf);
  }

  @After
  public void teardown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testChooseTargetNormalCase() throws IOException {
    String clientMachine = "client.foo.com";
    String src = "/testfile" + Time.monotonicNowNanos();
    // Create the file with client machine
    HdfsFileStatus fileStatus = namesystem.startFile(src, perm,
        clientMachine, clientMachine, EnumSet.of(CreateFlag.CREATE), true,
        (short) 9, DEFAULT_BLOCK_SIZE, null, null, null, false);

    //test chooseTarget for new file
    LocatedBlock locatedBlock = nameNodeRpc.addBlock(src, clientMachine,
        null, null, fileStatus.getFileId(), null, null);
    doTestLocatedBlock(9, locatedBlock.getLocations());

    //test chooseTarget for existing file.
    LocatedBlock additionalLocatedBlock = nameNodeRpc.getAdditionalDatanode(
        src, fileStatus.getFileId(), locatedBlock.getBlock(), locatedBlock.getLocations(),
        locatedBlock.getStorageIDs(), DatanodeInfo.EMPTY_ARRAY, 1, clientMachine);
    doTestLocatedBlock(10, additionalLocatedBlock.getLocations());
  }

  private void doTestLocatedBlock(int replication, DatanodeInfo[] locations) {
    assertEquals(replication, locations.length);

    HashSet<String> racks = new HashSet<>();
    HashSet<String> uds = new HashSet<>();
    for (DatanodeInfo node : locations) {
      racks.add(node.getNetworkLocation());
      uds.add(UpgradeDomainUtil.getUpgradeDomainWithDefaultValue(node));
    }

    assertEquals(replication, racks.size());
    assertEquals(replication, uds.size());
  }
}
