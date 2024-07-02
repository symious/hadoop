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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
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
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

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
    String dirName = "/test_zch";
    String pathName = dirName + "/testGetReplicaInfo1.txt";
    fs.mkdir(new Path(dirName), new FsPermission("777"));

    // write two files
    Path path1 = new Path(pathName);
    // client(127.0.0.1) will be mapped to a random node in (host0, host1, host2)
    DFSTestUtil.createFile(fs, path1, FILE_LEN, REPLICATION, 0L);
    Path path2 = new Path(dirName + "/testGetReplicaInfo2.txt");
    DFSTestUtil.createFile(fs, path2, FILE_LEN, REPLICATION, 0L);

    //Check the zone check for file
    Map<String, Short> replicaInfoMap = new HashMap<>();
    replicaInfoMap.put("/dc0", (short) 2);
    replicaInfoMap.put("/dc1", (short) 1);
    ReplicationRule replicationRule = ReplicationRule.parseFromMap(replicaInfoMap);

    final ZoneChecker zc = new ZoneChecker(conf);
    List<String> paths = new ArrayList<>();
    paths.add(path1.toUri().getPath());
    paths.add(path2.toUri().getPath());
    Map<String, Set<ReplicationRule>> dis1 = zc.getBlockDistribution(paths, 1);
    Map<String, Set<ReplicationRule>> dis2 = zc.getBlockDistribution(paths, 2);
    assertTrue(dis1.get(path1.toUri().getPath()).contains(replicationRule));
    assertTrue(dis1.get(path2.toUri().getPath()).contains(replicationRule));
    assertEquals(1, dis2.get(path1.toUri().getPath()).size());
    assertEquals(1, dis2.get(path2.toUri().getPath()).size());
    assertEquals(dis1, dis2);

    Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
    DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(namenodes.iterator().next(), conf);
    final ZoneChecker zch = new ZoneChecker(dfs, conf);


    Map<ReplicationRule, Set<String>> replicationRuleListMap =
        new HashMap<>();
    replicationRuleListMap.put(replicationRule, new HashSet<>(
        Collections.singletonList(pathName)));

    ConcurrentHashMap<ReplicationRule, Set<String>> rulePathMap = new ConcurrentHashMap<>();
    Map<String, List<Long>> dcStatMap = new HashMap<>();
    zch.check(pathName, rulePathMap, dcStatMap, null, false, false);
    assertEquals(replicationRuleListMap, rulePathMap);
    rulePathMap.clear();
    dcStatMap.clear();
    zch.concurrentlyCheck(pathName, rulePathMap, dcStatMap, null, false, false, 10);
    assertEquals(replicationRuleListMap, rulePathMap);

    //Check the zone check for dir
    replicationRuleListMap.clear();
    replicationRuleListMap.put(replicationRule, new HashSet<>(
        Collections.singletonList(dirName)));
    ConcurrentHashMap<ReplicationRule, Set<String>> rulePathMap1 = new ConcurrentHashMap<>();
    zch.check(dirName, rulePathMap1, dcStatMap, null, false, false);
    assertEquals(replicationRuleListMap, rulePathMap1);
    rulePathMap1.clear();
    dcStatMap.clear();
    zch.concurrentlyCheck(dirName, rulePathMap1, dcStatMap, null, false, false, 10);
    assertEquals(replicationRuleListMap, rulePathMap1);

    //Check the block summary
    Map<String, List<Long>> blockSummaryResult = new HashMap<>();
    blockSummaryResult.put("/dc0", Arrays.asList(4L, 4096L));
    blockSummaryResult.put("/dc1", Arrays.asList(2L, 2048L));
    Map<String, List<Long>> blockSummary = new HashMap<>();
    zch.check(dirName, rulePathMap1, blockSummary, null, true, false);
    assertEquals(blockSummaryResult, blockSummary);
    rulePathMap1.clear();
    blockSummary.clear();
    zch.concurrentlyCheck(dirName, rulePathMap1, blockSummary, null, true, false, 10);
    assertEquals(blockSummaryResult, blockSummary);

    //Check the block number summary
    Map<String, List<Long>> countResult = new HashMap<>();
    countResult.put(replicationRule.toString(), Arrays.asList(2L, 2048L));
    Map<String, List<Long>> countSummary = new HashMap<>();
    ZoneChecker.ZoneCheckerCountTree zcct =
        new ZoneChecker.ZoneCheckerCountTree(dirName, 0);
    zch.check(dirName, rulePathMap1, countSummary, zcct, false, true);
    assertEquals(countResult, zcct.getMap());
    countSummary.clear();
    zcct = new ZoneChecker.ZoneCheckerCountTree(dirName, 0);
    zch.concurrentlyCheck(dirName, rulePathMap1, countSummary, zcct, false, true, 10);
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
    DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(namenodes.iterator().next(), conf);
    final ZoneChecker zc = new ZoneChecker(dfs, conf);

    ConcurrentHashMap<ReplicationRule, Set<String>> rulePathMap;
    Map<String, List<Long>> dcStatMap;
    for (int trackedDepth = 1; trackedDepth < MAX_DEPTH; trackedDepth++) {
      rulePathMap = new ConcurrentHashMap<>();
      dcStatMap = new HashMap<>();
      ZoneChecker.ZoneCheckerCountTree zcct =
          new ZoneChecker.ZoneCheckerCountTree(basePath.toString(), trackedDepth);
      zc.check(basePath.toString(), rulePathMap, dcStatMap, zcct, false, true);
      checkCountTree(zcct.getRoot());
      ZoneChecker.printFileCount(zcct);

      rulePathMap.clear();
      dcStatMap.clear();
      zcct = new ZoneChecker.ZoneCheckerCountTree(basePath.toString(), trackedDepth);
      zc.concurrentlyCheck(basePath.toString(), rulePathMap, dcStatMap, zcct, false, true, 10);
      checkCountTree(zcct.getRoot());
      ZoneChecker.printFileCount(zcct);
    }

    rulePathMap = new ConcurrentHashMap<>();
    dcStatMap = new HashMap<>();
    zc.check(basePath.toString(), rulePathMap, dcStatMap, null, true, false);
    Map<String, List<Long>> blockSummaryResult = new HashMap<>();
    blockSummaryResult.put("/dc0", Arrays.asList((long) (1 << MAX_DEPTH + 1) - 2,
        (long) FILE_LEN * ((1 << MAX_DEPTH + 1) - 2)));
    blockSummaryResult.put("/dc1",
        Arrays.asList((long) (1 << MAX_DEPTH) - 1, (long) FILE_LEN * ((1 << MAX_DEPTH) - 1)));
    assertEquals(blockSummaryResult, dcStatMap);

    rulePathMap.clear();
    dcStatMap.clear();
    zc.concurrentlyCheck(basePath.toString(), rulePathMap, dcStatMap, null, true, false, 10);
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
      for (String rule: node.getBlockCounts().keySet()) {
        currentNodeBlockCounts += node.getBlockCounts().get(rule);
        currentNodeByteCounts += node.getByteCounts().get(rule);
      }

      long childBlockCounts = 0;
      long childByteCounts = 0;
      for (String child: node.children.keySet()) {
        ZoneChecker.ZoneCheckerCountTreeNode childNode = node.children.get(child);
        for (String rule: childNode.getBlockCounts().keySet()) {
          childBlockCounts += childNode.getBlockCounts().get(rule);
          childByteCounts += childNode.getByteCounts().get(rule);
        }
      }
      assertEquals(childBlockCounts, currentNodeBlockCounts);
      assertEquals(childByteCounts, currentNodeByteCounts);
    }
  }

  @Test
  public void testZoneCheckerCountTreeNodeSingleFile() {
    int DEPTH = 5;
    ZoneChecker.ZoneCheckerCountTree zcct = new ZoneChecker.ZoneCheckerCountTree("/", DEPTH);
    zcct.addNode("/1/2/3/4/5/6/7/8.file", null, 1, 1);

    int currentDepth = 0;
    ZoneChecker.ZoneCheckerCountTreeNode parent = null;
    ZoneChecker.ZoneCheckerCountTreeNode cursor = zcct.getRoot();
    while (currentDepth < DEPTH) {
      assertEquals(1, cursor.children.size());
      assertSame(cursor.parent, parent);
      parent = cursor;
      cursor = cursor.children.values().iterator().next();
      currentDepth++;
    }
    assertEquals(0, cursor.children.size());
  }
}