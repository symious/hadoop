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
package org.apache.hadoop.hdfs.server.zoneservice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.net.DFSNetworkTopology;
import org.apache.hadoop.hdfs.net.DFSNetworkTopologyWithDataCenter;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyWithDataCenter;
import org.apache.hadoop.hdfs.server.zoneservice.utils.MigrationDataCenters;
import org.apache.hadoop.net.StaticMapping;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestZoneMoverWithSetReplication {
  private MiniDFSCluster cluster = null;
  private static final long FILE_LEN = 1024;
  private static final short REPLICATION = 3;
  private static final Logger LOG =
      LoggerFactory.getLogger(TestZoneMoverWithSetReplication.class);
  public static final String TEST_CACHE_DATA_DIR =
      System.getProperty("test.cache.data", "build/test/cache");

  private static final String DCs =
      MigrationDataCenters.AT.getName() + "," + MigrationDataCenters.TL.getName() + ","
          + MigrationDataCenters.STT.getName();

  private final Map<Integer, ReplicationRule> ruleAlias = new HashMap<>();
  Map<ReplicationRule, ReplicationRule> referMap = new HashMap<>();

  private void init() {
    if (!ruleAlias.isEmpty()) {
      return;
    }
    ruleAlias.put(1, ReplicationRule.parseFromString(String.format("%s:1",
        MigrationDataCenters.AT)));
    ruleAlias.put(10, ReplicationRule.parseFromString(String.format("%s:1",
        MigrationDataCenters.TL)));
    ruleAlias.put(100, ReplicationRule.parseFromString(String.format("%s:1",
        MigrationDataCenters.STT)));
    ruleAlias.put(2, ReplicationRule.parseFromString(String.format("%s:2",
        MigrationDataCenters.AT)));
    ruleAlias.put(20, ReplicationRule.parseFromString(String.format("%s:2",
        MigrationDataCenters.TL)));
    ruleAlias.put(200, ReplicationRule.parseFromString(String.format("%s:2",
        MigrationDataCenters.STT)));
    ruleAlias.put(11, ReplicationRule.parseFromString(String.format("%s:1,%s:1",
        MigrationDataCenters.TL, MigrationDataCenters.AT)));
    ruleAlias.put(110, ReplicationRule.parseFromString(String.format("%s:1,%s:1",
        MigrationDataCenters.STT, MigrationDataCenters.TL)));
    ruleAlias.put(101, ReplicationRule.parseFromString(String.format("%s:1,%s:1",
        MigrationDataCenters.STT, MigrationDataCenters.AT)));
    ruleAlias.put(111, ReplicationRule.parseFromString(String.format("%s:1,%s:1,%s:1",
        MigrationDataCenters.STT, MigrationDataCenters.TL, MigrationDataCenters.AT)));
    ruleAlias.put(21, ReplicationRule.parseFromString(String.format("%s:2,%s:1",
        MigrationDataCenters.TL, MigrationDataCenters.AT)));
    ruleAlias.put(12, ReplicationRule.parseFromString(String.format("%s:1,%s:2",
        MigrationDataCenters.TL, MigrationDataCenters.AT)));
    ruleAlias.put(3, ReplicationRule.parseFromString(String.format("%s:3",
        MigrationDataCenters.AT)));
    ruleAlias.put(30, ReplicationRule.parseFromString(String.format("%s:3",
        MigrationDataCenters.TL)));
    ruleAlias.put(112, ReplicationRule.parseFromString(String.format("%s:1,%s:1,%s:2",
        MigrationDataCenters.STT, MigrationDataCenters.TL, MigrationDataCenters.AT)));
    ruleAlias.put(220, ReplicationRule.parseFromString(String.format("%s:2,%s:2",
        MigrationDataCenters.STT, MigrationDataCenters.TL)));
    ruleAlias.put(221, ReplicationRule.parseFromString(String.format("%s:2,%s:2,%s:1",
        MigrationDataCenters.STT, MigrationDataCenters.TL, MigrationDataCenters.AT)));

    referMap.put(ruleAlias.get(1), ruleAlias.get(1));
    referMap.put(ruleAlias.get(10), ruleAlias.get(100));
    referMap.put(ruleAlias.get(2), ruleAlias.get(2));
    referMap.put(ruleAlias.get(20), ruleAlias.get(110));
    referMap.put(ruleAlias.get(200), ruleAlias.get(200));
    referMap.put(ruleAlias.get(11), ruleAlias.get(111));
    referMap.put(ruleAlias.get(110), ruleAlias.get(110));
    referMap.put(ruleAlias.get(101), ruleAlias.get(101));
    // 3-replica has replica in STT
    referMap.put(ruleAlias.get(111), ruleAlias.get(111));
    referMap.put(ruleAlias.get(201), ruleAlias.get(111));
    referMap.put(ruleAlias.get(210), ruleAlias.get(111));
    referMap.put(ruleAlias.get(102), ruleAlias.get(111));
    referMap.put(ruleAlias.get(120), ruleAlias.get(111));
    referMap.put(ruleAlias.get(300), ruleAlias.get(300));
    // AT:3
    referMap.put(ruleAlias.get(3), ruleAlias.get(3));
    // TL:3
    referMap.put(ruleAlias.get(30), ruleAlias.get(220));
    referMap.put(ruleAlias.get(12), ruleAlias.get(112));
    referMap.put(ruleAlias.get(21), ruleAlias.get(221));
  }

  @Test
  public void testBatchMode() throws Exception {
    init();
    // construct a cluster with three datacenters
    StaticMapping.resetMap();
    final String[] hosts1 = {"host0", "host1", "host2", "host3", "host4", "host10", "host11",
        "host12", "host13", "host14", "host20", "host21", "host22", "host23", "host24"};
    final String[] racks1 = {MigrationDataCenters.TL + "/rack0",
        MigrationDataCenters.TL + "/rack2", MigrationDataCenters.TL + "/rack1",
        MigrationDataCenters.TL + "/rack2", MigrationDataCenters.TL + "/rack1",
        MigrationDataCenters.AT + "/rack0", MigrationDataCenters.AT + "/rack2",
        MigrationDataCenters.AT + "/rack0", MigrationDataCenters.AT + "/rack2",
        MigrationDataCenters.AT + "/rack1", MigrationDataCenters.STT + "/rack2",
        MigrationDataCenters.STT + "/rack0", MigrationDataCenters.STT + "/rack1",
        MigrationDataCenters.STT + "/rack0", MigrationDataCenters.STT + "/rack1"};
    Configuration conf = TestUtils.getConf(DCs);
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, REPLICATION);
    conf.set(DFSConfigKeys.DFS_ZONEMOVER_DISTRIBUTION_RULE_MAP_FILE_KEY, TEST_CACHE_DATA_DIR + "/distribution.map");
    conf.set(DFSConfigKeys.DFS_ZONEMOVER_DEGRADE_RULE_MAP_FILE_KEY, TEST_CACHE_DATA_DIR + "/degrade.map");
    conf.set(DFSConfigKeys.DFS_ZONEMOVER_DEFAULT_RULE_MAP_FILE_KEY, TEST_CACHE_DATA_DIR + "/default.map");
    cluster = new MiniDFSCluster
        .Builder(conf)
        .numDataNodes(hosts1.length).hosts(hosts1).racks(racks1).build();
    cluster.waitActive();
    DistributedFileSystem fs = cluster.getFileSystem();
    fs.mkdir(new Path("/test"), new FsPermission("777"));

    final int[] listTest = {1, 10, 2, 20, 11, 3, 30, 21, 12};
    List<Path> pathList = new ArrayList<>();

    // Prepare the files with different distribution
    for (int disNum: listTest) {
      int tmp = disNum;
      short replication = 0;
      while (tmp != 0) {
        replication += tmp % 10;
        tmp = tmp / 10;
      }
      Path path = new Path("/test/testBatchFile." + disNum);
      DFSTestUtil.createFile(fs, path, FILE_LEN, replication, 0L);
      pathList.add(path);

      ZoneMover.run(conf, cluster.getURI(), Collections.singletonList(path), ruleAlias.get(disNum));
      LOG.info("LEO check the first step will apply the rule {} on {}", ruleAlias.get(disNum), path);
    }
    Thread.sleep(TestUtils.DFS_HEARTBEAT_INTERVAL * 10 * 1000);

    for (int disNum: listTest) {
      LOG.info("Precheck file: " + disNum);
      Path path = new Path("/test/testBatchFile." + disNum);
      List<LocatedBlock> blocks = DFSTestUtil.getAllBlocks(fs, path);
      assertEquals(ruleAlias.get(disNum),
          ReplicationRule.parseFromMap(ZoneMover.getBlockDistribution(blocks.get(0))));
      LOG.info("Successfully file is {}", path.getName());
    }
    // Run the batch mode for the directory
    ZoneMoverWithSetReplication.runWithSetReplication(conf, cluster.getURI(), pathList);
    Thread.sleep(TestUtils.DFS_HEARTBEAT_INTERVAL * 10 * 1000);

    // Check the move result
    for (int disNum: listTest) {
      LOG.info("Checking file: " + disNum);
      Path path = new Path("/test/testBatchFile." + disNum);
      List<LocatedBlock> blocks = DFSTestUtil.getAllBlocks(fs, path);
      assertEquals(referMap.get(ruleAlias.get(disNum)),
          ReplicationRule.parseFromMap(ZoneMover.getBlockDistribution(blocks.get(0))));
    }


  }

  @Test
  public void testCheckMode() throws Exception {
    init();
    // construct a cluster with three datacenters
    StaticMapping.resetMap();
    final String[] hosts1 = {"host0", "host1", "host2", "host3", "host4", "host10", "host11",
        "host12", "host13", "host14", "host20", "host21", "host22", "host23", "host24"};
    final String[] racks1 = {MigrationDataCenters.TL + "/rack0",
        MigrationDataCenters.TL + "/rack2", MigrationDataCenters.TL + "/rack1",
        MigrationDataCenters.TL + "/rack2", MigrationDataCenters.TL + "/rack3",
        MigrationDataCenters.AT + "/rack0", MigrationDataCenters.AT + "/rack2",
        MigrationDataCenters.AT + "/rack0", MigrationDataCenters.AT + "/rack3",
        MigrationDataCenters.AT + "/rack1", MigrationDataCenters.STT + "/rack2",
        MigrationDataCenters.STT + "/rack0", MigrationDataCenters.STT + "/rack1",
        MigrationDataCenters.STT + "/rack0", MigrationDataCenters.STT + "/rack3"};
    Configuration conf = TestUtils.getConf(DCs);
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, REPLICATION);
    conf.set(DFSConfigKeys.DFS_ZONEMOVER_DISTRIBUTION_RULE_MAP_FILE_KEY, TEST_CACHE_DATA_DIR + "/distribution.map");
    conf.set(DFSConfigKeys.DFS_ZONEMOVER_DEGRADE_RULE_MAP_FILE_KEY, TEST_CACHE_DATA_DIR + "/degrade.map");
    conf.set(DFSConfigKeys.DFS_ZONEMOVER_DEFAULT_RULE_MAP_FILE_KEY, TEST_CACHE_DATA_DIR + "/default.map");
    cluster = new MiniDFSCluster
        .Builder(conf)
        .numDataNodes(hosts1.length).hosts(hosts1).racks(racks1).build();
    cluster.waitActive();
    DistributedFileSystem fs = cluster.getFileSystem();
    fs.mkdir(new Path("/test"), new FsPermission("777"));

    final int[] listTest = {2, 20, 11, 3, 30, 21, 12, 220};
    List<Path> pathList = new ArrayList<>();

    // Prepare the files with different distribution
    for (int disNum: listTest) {
      int tmp = disNum;
      short replication = 0;
      while (tmp != 0) {
        replication += tmp % 10;
        tmp = tmp / 10;
      }
      Path path = new Path("/test/testFile." + disNum);
      DFSTestUtil.createFile(fs, path, FILE_LEN, replication, 0L);
      pathList.add(path);

      try {
        ZoneMover.run(conf, cluster.getURI(), Collections.singletonList(path),
            ruleAlias.get(disNum));
      } catch (NullPointerException e) {
        System.out.println("Leo check the null disNum is " + disNum);
      }
    }
    Thread.sleep(TestUtils.DFS_HEARTBEAT_INTERVAL * 10 * 1000);

    for (int disNum: listTest) {
      LOG.info("Precheck file: " + disNum);
      Path path = new Path("/test/testFile." + disNum);
      List<LocatedBlock> blocks = DFSTestUtil.getAllBlocks(fs, path);
      assertEquals(ruleAlias.get(disNum),
          ReplicationRule.parseFromMap(ZoneMover.getBlockDistribution(blocks.get(0))));
    }

    // Run the check mode for the directory
    ZoneMoverWithSetReplication.checkWithSetReplication(conf, cluster.getURI(),
        pathList, MigrationDataCenters.AT);
    Thread.sleep(TestUtils.DFS_HEARTBEAT_INTERVAL * 10 * 1000);

    // Check the move result
    for (int disNum: listTest) {
      LOG.info("Checking file: " + disNum);
      Path path = new Path("/test/testFile." + disNum);
      List<LocatedBlock> blocks = DFSTestUtil.getAllBlocks(fs, path);
      assertTrue(ZoneMover.getBlockDistribution(blocks.get(0)).containsKey(
          MigrationDataCenters.AT.getName()));
    }
  }

  @After
  public void teardown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }
}
