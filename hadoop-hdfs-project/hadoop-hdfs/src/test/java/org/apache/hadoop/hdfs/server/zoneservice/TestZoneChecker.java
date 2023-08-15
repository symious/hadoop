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
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class TestZoneChecker {
  private static final long FILE_LEN = 1024;
  private static final short REPLICATION = 3;

  @Test
  public void testGetReplicaInfo() throws Exception {
    final String[] hosts1 = {"host0", "host1", "host2"};
    final String[] racks1 = {"/dc1/rack0", "/dc0/rack0", "/dc0/rack1"};

    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(CommonConfigurationKeys.IGNORE_SDI_AUTHENTICATE_KEY, true);
    MiniDFSCluster cluster = new MiniDFSCluster
        .Builder(conf)
        .numDataNodes(hosts1.length).hosts(hosts1).racks(racks1).build();
    cluster.waitActive();
    DistributedFileSystem fs = cluster.getFileSystem();
    String dirName = "/test_zch/";
    String pathName = dirName + "testGetReplicaInfo1.txt";
    fs.mkdir(new Path(dirName), new FsPermission("777"));

    // write two files
    Path path1 = new Path(pathName);
    // client(127.0.0.1) will be mapped to a random node in (host0, host1, host2)
    DFSTestUtil.createFile(fs, path1, FILE_LEN, REPLICATION, 0L);
    DFSTestUtil.createFile(fs, new Path(dirName + "testGetReplicaInfo2.txt"),
        FILE_LEN, REPLICATION, 0L);

    NameNodeConnector nnc;
    Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
    nnc = new NameNodeConnector(namenodes.iterator().next(),
        Collections.singletonList(new Path(pathName)), conf, 1);
    final ZoneChecker zch = new ZoneChecker(nnc, conf);

    //Check the zone check for file
    Map<String, Short> replicaInfoMap = new HashMap<>();
    replicaInfoMap.put("/dc0", (short) 2);
    replicaInfoMap.put("/dc1", (short) 1);
    ReplicationRule replicationRule =
        ReplicationRule.parseFromMap(replicaInfoMap);
    Map<ReplicationRule, Set<String>> replicationRuleListMap =
        new HashMap<>();
    replicationRuleListMap.put(replicationRule, new HashSet<>(
        Collections.singletonList(pathName)));

    Map<ReplicationRule, Set<String>> rulePathMap = new HashMap<>();
    Map<String, List<Long>> dcStatMap = new HashMap<>();
    zch.getReplicaInfo(pathName, rulePathMap, dcStatMap, null, false, false);
    assertEquals(replicationRuleListMap, rulePathMap);

    //Check the zone check for dir
    replicationRuleListMap.clear();
    replicationRuleListMap.put(replicationRule, new HashSet<>(
        Collections.singletonList(dirName)));
    Map<ReplicationRule, Set<String>> rulePathMap1 = new HashMap<>();
    zch.getReplicaInfo(dirName, rulePathMap1, dcStatMap, null, false, false);
    assertEquals(replicationRuleListMap, rulePathMap1);

    //Check the block summary
    Map<String, List<Long>> blockSummaryResult = new HashMap<>();
    blockSummaryResult.put("/dc0", Arrays.asList(4L, 4096L));
    blockSummaryResult.put("/dc1", Arrays.asList(2L, 2048L));
    Map<String, List<Long>> blockSummary = new HashMap<>();
    zch.getReplicaInfo(dirName, rulePathMap1, blockSummary, null, true, false);
    assertEquals(blockSummaryResult, blockSummary);

    //Check the block number summary
    Map<String, List<Long>> countResult = new HashMap<>();
    countResult.put(replicationRule.toString(), Arrays.asList(2L, 2048L));
    Map<String, List<Long>> countSummary = new HashMap<>();
    ZoneChecker.ZoneCheckerCountTree zcct =
        new ZoneChecker.ZoneCheckerCountTree(dirName, 0);
    zch.getReplicaInfo(dirName, rulePathMap1, countSummary, zcct, false, true);
    assertEquals(countResult, zcct.getMap());
  }

  @Test
  public void testGetReplicaInfoByDepth() throws IOException {
    final String[] hosts1 = { "host0", "host1", "host2" };
    final String[] racks1 = { "/dc1/rack0", "/dc0/rack0", "/dc0/rack1" };

    final int MAX_DEPTH =
        4; // Test will create 2^MAX_DEPTH directories, don't go hard with this var

    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(CommonConfigurationKeys.IGNORE_SDI_AUTHENTICATE_KEY, true);
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(hosts1.length).hosts(hosts1).racks(racks1)
            .build();
    cluster.waitActive();
    DistributedFileSystem fs = cluster.getFileSystem();
    Path basePath = new Path("/testGetReplicaInfoByDepth/");
    fs.mkdir(basePath, new FsPermission("777"));
    createChildAndSubdirsRecursively(fs, 0, MAX_DEPTH, basePath);

    Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
    NameNodeConnector nnc =
        new NameNodeConnector(namenodes.iterator().next(), Collections.singletonList(basePath),
            conf, 1);
    ZoneChecker zc = new ZoneChecker(nnc, conf);

    Map<ReplicationRule, Set<String>> rulePathMap;
    Map<String, List<Long>> dcStatMap;
    for (int trackedDepth = 1; trackedDepth < MAX_DEPTH; trackedDepth++) {
      rulePathMap = new HashMap<>();
      dcStatMap = new HashMap<>();
      ZoneChecker.ZoneCheckerCountTree zcct =
          new ZoneChecker.ZoneCheckerCountTree(basePath.toString(), trackedDepth);
      zc.getReplicaInfo(basePath.toString(), rulePathMap, dcStatMap, zcct, false, true);
      checkCountTree(zcct.root);
      ZoneChecker.printFileCount(zcct);
    }

    rulePathMap = new HashMap<>();
    dcStatMap = new HashMap<>();
    zc.getReplicaInfo(basePath.toString(), rulePathMap, dcStatMap, null, true, false);
    Map<String, List<Long>> blockSummaryResult = new HashMap<>();
    blockSummaryResult.put("/dc0", Arrays.asList((long) (1 << MAX_DEPTH + 1) - 2,
        (long) FILE_LEN * ((1 << MAX_DEPTH + 1) - 2)));
    blockSummaryResult.put("/dc1",
        Arrays.asList((long) (1 << MAX_DEPTH) - 1, (long) FILE_LEN * ((1 << MAX_DEPTH) - 1)));
    assertEquals(blockSummaryResult, dcStatMap);
  }

  private void createChildAndSubdirsRecursively(DistributedFileSystem fs, int depth, int maxDepth,
      Path currentPath) throws IOException {
    depth++;
    DFSTestUtil.createFile(fs, new Path(currentPath, "m.txt"), FILE_LEN, REPLICATION, 0);
    if (depth == maxDepth) {
      return;
    }
    Path lPath = new Path(currentPath, "l");
    Path rPath = new Path(currentPath, "r");
    fs.mkdir(lPath, new FsPermission("777"));
    fs.mkdir(rPath, new FsPermission("777"));
    createChildAndSubdirsRecursively(fs, depth, maxDepth, lPath);
    createChildAndSubdirsRecursively(fs, depth, maxDepth, rPath);
  }

  private void checkCountTree(ZoneChecker.ZoneCheckerCountTreeNode root) {
    ZoneChecker.ZoneCheckerCountTreeNode node = root;
    if (node != null && node.children.size() != 0) {
      long currentNodeBlockCounts = 0;
      long currentNodeByteCounts = 0;
      for (String rule: node.blockCounts.keySet()) {
        currentNodeBlockCounts += node.blockCounts.get(rule);
        currentNodeByteCounts += node.byteCounts.get(rule);
      }

      long childBlockCounts = 0;
      long childByteCounts = 0;
      for (String child: node.children.keySet()) {
        ZoneChecker.ZoneCheckerCountTreeNode childNode = node.children.get(child);
        for (String rule: childNode.blockCounts.keySet()) {
          childBlockCounts += childNode.blockCounts.get(rule);
          childByteCounts += childNode.byteCounts.get(rule);
        }
      }
      assertEquals(childBlockCounts, currentNodeBlockCounts);
      assertEquals(childByteCounts, currentNodeByteCounts);
    }
  }
}