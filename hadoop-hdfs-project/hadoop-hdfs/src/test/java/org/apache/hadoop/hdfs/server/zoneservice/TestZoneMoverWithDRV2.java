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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.net.DFSNetworkTopology;
import org.apache.hadoop.hdfs.net.DFSNetworkTopologyWithDataCenter;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyWithDataCenter;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.hdfs.server.zoneservice.metrics.ZoneMoverMetrics;
import org.apache.hadoop.hdfs.server.zoneservice.metrics.ZoneProgressTracker;
import org.apache.hadoop.hdfs.server.zoneservice.metrics.ZoneServiceMetrics;
import org.apache.hadoop.hdfs.server.zoneservice.store.KafkaTopicRecord;
import org.apache.hadoop.hdfs.server.zoneservice.store.StoreDriver;
import org.apache.hadoop.hdfs.server.zoneservice.utils.RunMode;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestZoneMoverWithDRV2 {
  private static final Logger LOG = LoggerFactory.getLogger(TestZoneMoverWithDRV2.class);
  private static final long FILE_LEN = 1024;
  private static final int DEFAULT_BLOCK_SIZE = 1024 * 1024;
  private static final ErasureCodingPolicy ecPolicy = StripedFileTestUtil.getDefaultECPolicy();
  private static final short parityBlocks = (short) ecPolicy.getNumParityUnits();

  @BeforeClass
  public static void setLogging() {
    LogManager.getLogger(ZoneProgressTracker.class.getName()).setLevel(Level.DEBUG);
    LogManager.getLogger(ZoneMoverWithDRV2.class.getName()).setLevel(Level.DEBUG);
    LogManager.getLogger("org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy").setLevel(Level.DEBUG);
    LogManager.getLogger("org.apache.hadoop.net.NetworkTopology").setLevel(Level.DEBUG);
  }

  private void initConfForDr(Configuration conf,
      String defaultDataCenter, long drColdDataThresholdMS) {
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY, false);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setClass(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
        BlockPlacementPolicyWithDataCenter.class,
        BlockPlacementPolicy.class);
    conf.setBoolean(DFSConfigKeys.DFS_USE_DFS_NETWORK_TOPOLOGY_KEY, true);
    conf.setClass(DFSConfigKeys.DFS_NET_TOPOLOGY_IMPL_KEY,
        DFSNetworkTopologyWithDataCenter.class, DFSNetworkTopology.class);
    conf.set(DFSConfigKeys.DFS_NAMENODE_BLOCK_PLACEMENT_POLICY_WITH_DATA_CENTER_FALLBACK_DC_KEY,
        defaultDataCenter);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 500);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY, 10);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.setInt(DFSConfigKeys.DFS_ZONEMOVER_TRIGGER_ZK_UPDATE_OFFSET_INTERVAL_KEY, 0);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_DR_REPLICATION_RULE_ENABLE_KEY, true);
    conf.set(DFSConfigKeys.DFS_NAMENODE_DR_DATACENTERS_KEY, "/datacenter0,/datacenter1");
    conf.set(DFSConfigKeys.DFS_NAMENODE_DR_REPLICATION_RULE_COLD_DATA_KEY,
        "3=/datacenter0:1,/datacenter1:2");
    conf.set(DFSConfigKeys.DFS_NAMENODE_DR_STRIPED_BLOCK_RULE_KEY,
        "9=/datacenter0:6,/datacenter1:3");
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_DR_COLD_DATA_THRESHOLD_MS_KEY,
        drColdDataThresholdMS);
  }

  @Test
  public void testZoneMoverWithDRCli() throws Exception {
    GenericTestUtils.LogCapturer logs =
        GenericTestUtils.LogCapturer.captureLogs(ZoneMoverWithDRV2.LOG);

    Configuration conf = new HdfsConfiguration();
    final String[] racks = {"/datacenter0/rack0", "/datacenter0/rack1", "/datacenter0/rack2"
        , "/datacenter0/rack3", "/datacenter0/rack4", "/datacenter0/rack5",
        "/datacenter0/rack6", "/datacenter0/rack7", "/datacenter0/rack8"};
    final String[] hosts = {"host0", "host1", "host2", "host10", "host11", "host12",
        "host20", "host21", "host22"};
    initConfForDr(conf, "/datacenter0", 0);
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).
        nnTopology(MiniDFSNNTopology.simpleHATopology()).
        numDataNodes(hosts.length).hosts(hosts).racks(racks).build()) {
      HATestUtil.setFailoverConfigurations(cluster, conf, "dev1");
      cluster.waitActive();
      cluster.transitionToActive(0);

      // Spy tool.
      ZoneMoverWithDRV2.Cli tool = Mockito.spy(new ZoneMoverWithDRV2.Cli());

      tool.setConf(conf);

      // Validate param '-cold' and '-monitorByTrigger' must specify one.
      String[] args = {"-namespace", "dev", "-path", "/test"};
      assertEquals(ExitStatus.ILLEGAL_ARGUMENTS.getExitCode(), tool.run(args));

      // Unable to match namespace.
      String[] args1 = {"-namespace", "dev", "-path", "/test", "-cold"};
      assertEquals(ExitStatus.ILLEGAL_ARGUMENTS.getExitCode(), tool.run(args1));

      // Invalid path.
      String[] args2 = {"-namespace", "dev1", "-path", "test", "-cold"};
      assertEquals(ExitStatus.ILLEGAL_ARGUMENTS.getExitCode(), tool.run(args2));

      DistributedFileSystem fs = cluster.getFileSystem(0);

      short replication = 2;
      Path path1 = new Path("/test1/coldFile");
      DFSTestUtil.createFile(fs, path1, FILE_LEN, replication, 0L);
      DFSTestUtil.waitReplication(fs, path1, replication);
      assertEquals(ReplicationRule.parseFromMap(ZoneUtil.getBlockDistribution(
              DFSTestUtil.getAllBlocks(fs, path1).get(0))),
          ReplicationRule.parseFromString("/datacenter0:2"));

      replication = 3;
      Path path2 = new Path("/test2/coldFile");
      DFSTestUtil.createFile(fs, path2, FILE_LEN, replication, 0L);
      DFSTestUtil.waitReplication(fs, path2, replication);
      assertEquals(ReplicationRule.parseFromMap(ZoneUtil.getBlockDistribution(
              DFSTestUtil.getAllBlocks(fs, path2).get(0))),
          ReplicationRule.parseFromString("/datacenter0:3"));

      // Create ec dir.
      Path ecDir = new Path("/ec");
      fs.mkdirs(ecDir);
      fs.enableErasureCodingPolicy(ecPolicy.getName());
      fs.setErasureCodingPolicy(ecDir, ecPolicy.getName());
      // Create ec file.
      Path ecFile = new Path(ecDir, "file");
      int dataSize = DEFAULT_BLOCK_SIZE * 6;
      byte[] expected = StripedFileTestUtil.generateBytes(dataSize);
      DFSTestUtil.writeFile(fs, ecFile, new String(expected));
      StripedFileTestUtil.waitBlockGroupsReported(fs, ecFile.toString());
      StripedFileTestUtil.verifyLength(fs, ecFile, dataSize);
      assertEquals(ReplicationRule.parseFromMap(ZoneUtil.getBlockDistribution(
              DFSTestUtil.getAllBlocks(fs, ecFile).get(0))),
          ReplicationRule.parseFromString("/datacenter0:9"));

      // Adding 6 new hosts about '/datacenter1'.
      cluster.startDataNodes(conf, 3, true, null,
          new String[]{"/datacenter1/rack0", "/datacenter1/rack1", "/datacenter1/rack2"},
          new String[]{"host6", "host7", "host8"},
          null);
      cluster.triggerBlockReports();
      assertEquals("Number of datanodes should be 12", 12,
          cluster.getDataNodes().size());

      // Validate use "-path" replica rule.
      ZoneMoverWithDRV2.Cli tool1 = new ZoneMoverWithDRV2.Cli();
      tool1.setConf(conf);
      String[] args3 = {"-namespace", "dev1", "-path", "/test2", "-cold", "-useAccessTime"};
      assertEquals(ExitStatus.SUCCESS.getExitCode(), tool1.run(args3));

      GenericTestUtils.waitFor(() -> {
        try {
          return ReplicationRule.parseFromMap(ZoneUtil.getBlockDistribution(
                  DFSTestUtil.getAllBlocks(fs, path2).get(0))).
              equals(ReplicationRule.parseFromString("/datacenter0:1,/datacenter1:2"));
        } catch (IOException e) {
          return false;
        }
      }, 500, 30000);

      // Validate use "-pathFile" replica rule.
      File inputPath = File.createTempFile("test", ".txt");
      BufferedWriter inputWriter =
          new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(inputPath.toPath())));
      inputWriter.write(path1.toString());
      inputWriter.write("\n");
      inputWriter.write(ecFile.toString());
      inputWriter.flush();
      inputWriter.close();

      String[] args4 = {"-namespace", "dev1", "-pathFile", inputPath.getAbsolutePath(), "-cold",
          "-skipEC", "-skipCheckCold"};
      assertEquals(ExitStatus.SUCCESS.getExitCode(), tool1.run(args4));

      // Validate replica rule.
      GenericTestUtils.waitFor(() -> {
        try {
          return ReplicationRule.parseFromMap(ZoneUtil.getBlockDistribution(
                  DFSTestUtil.getAllBlocks(fs, path1).get(0))).
              equals(ReplicationRule.parseFromString("/datacenter0:1,/datacenter1:1"));
        } catch (IOException e) {
          return false;
        }
      }, 500, 30000);

      // Since -skipEC is specified, the ec file will be skipped.
      assertTrue(logs.getOutput().contains(String.format("No need to process data for ec " +
          "file: %s.", ecFile)));

      // Validate param '-skipReplica' and '-skipEC' cannot be specified together,
      // can either specify one or leave both unspecified.
      String[] args5 = {"-namespace", "dev1", "-path", "/test2", "-cold", "-skipEC",
          "-skipReplica"};
      assertEquals(ExitStatus.ILLEGAL_ARGUMENTS.getExitCode(), tool1.run(args5));

      String[] args6 = {"-namespace", "dev1", "-path", "/ec/file", "-cold", "-useAccessTime"};
      assertEquals(ExitStatus.SUCCESS.getExitCode(), tool1.run(args6));

      // Validate striped block rule.
      GenericTestUtils.waitFor(() -> {
        try {
          return ReplicationRule.parseFromMap(ZoneUtil.getBlockDistribution(
                  DFSTestUtil.getAllBlocks(fs, ecFile).get(0))).
              equals(ReplicationRule.parseFromString("/datacenter0:6,/datacenter1:3"));
        } catch (IOException e) {
          return false;
        }
      }, 500, 30000);

      // Validate ec data.
      DFSClient dfsClient = fs.getClient();
      int done = 0;
      ByteBuffer readBuffer = ByteBuffer.allocate(dataSize);
      try (DFSInputStream in = dfsClient.open(ecFile.toString())) {
        while (done < dataSize) {
          int ret = in.read(readBuffer);
          assertTrue(ret > 0);
          done += ret;
        }
        assertArrayEquals(expected, readBuffer.array());
      }
    }
  }

  @Test
  public void testZoneMoverWithDRColdMode() throws Exception {

    Configuration conf = new HdfsConfiguration();
    final String[] racks = {"/datacenter0/rack0", "/datacenter0/rack0", "/datacenter0/rack0",
        "/datacenter0/rack1", "/datacenter0/rack1", "/datacenter0/rack1"};
    final String[] hosts = {"host0", "host1", "host2", "host3", "host4", "host5"};
    initConfForDr(conf, "/datacenter0", 0);

    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).
        numDataNodes(hosts.length).hosts(hosts).racks(racks).build()) {
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      fs.mkdir(new Path("/test"), new FsPermission("777"));

      final int[] listTest = {2, 3, 4, 5, 6};
      List<Path> pathList = new ArrayList<>();
      // Prepare the files with different distribution.
      for (int disNum : listTest) {
        short replication = (short) disNum;
        Path path = new Path("/test/coldFile." + disNum);
        DFSTestUtil.createFile(fs, path, FILE_LEN, replication, 0L);
        DFSTestUtil.waitReplication(fs, path, replication);
        pathList.add(path);
      }

      // Adding 6 new hosts about '/datacenter1'.
      cluster.startDataNodes(conf, 6, true, null,
          new String[]{"/datacenter1/rack0", "/datacenter1/rack0", "/datacenter1/rack0",
              "/datacenter1/rack1", "/datacenter1/rack1", "/datacenter1/rack1"},
          new String[]{"host6", "host7", "host8", "host9", "host10", "host11"},
          null);
      cluster.triggerBlockReports();

      ZoneMoverWithDRV2.runWithColdDataReplication(conf, cluster.getURI(), pathList,
          false, false, false, false);

      Map<Short, ReplicationRule> expectedRule = new HashMap<>();
      expectedRule.put((short) 2, ReplicationRule.parseFromString("/datacenter0:1,/datacenter1:1"));
      expectedRule.put((short) 3, ReplicationRule.parseFromString("/datacenter0:1,/datacenter1:2"));
      expectedRule.put((short) 4, ReplicationRule.parseFromString("/datacenter1:2,/datacenter0:2"));
      expectedRule.put((short) 5, ReplicationRule.parseFromString("/datacenter1:2,/datacenter0:3"));
      expectedRule.put((short) 6, ReplicationRule.parseFromString("/datacenter1:3,/datacenter0:3"));

      // Validate replica rule.
      for (Path path : pathList) {
        GenericTestUtils.waitFor(() -> {
          try {
            return expectedRule.get(fs.getFileStatus(path).getReplication()).equals(
                ReplicationRule.parseFromMap(ZoneMover.getBlockDistribution(
                    DFSTestUtil.getAllBlocks(fs, path).get(0))));
          } catch (IOException e) {
            return false;
          }
        }, 500, 50000);
      }
    }
  }

  @Test
  public void testZoneMoverWithDRColdModeByEC() throws Exception {
    Configuration conf = new HdfsConfiguration();
    final String[] racks = {"/datacenter0/rack0", "/datacenter0/rack1", "/datacenter0/rack2"
        , "/datacenter0/rack3", "/datacenter0/rack4", "/datacenter0/rack5",
        "/datacenter0/rack6", "/datacenter0/rack7", "/datacenter0/rack8"};
    final String[] hosts = {"host0", "host1", "host2", "host10", "host11", "host12",
        "host20", "host21", "host22"};
    initConfForDr(conf, "/datacenter0", 0);

    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).
        numDataNodes(hosts.length).hosts(hosts).racks(racks).build()) {
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      // Create ec dir.
      Path ecDir = new Path("/ec");
      fs.mkdirs(ecDir);
      fs.enableErasureCodingPolicy(ecPolicy.getName());
      fs.setErasureCodingPolicy(ecDir, ecPolicy.getName());

      final int[] listTest = {3, 5 ,6};
      Map<Integer, Path> map = new HashMap<>();

      // Prepare the files with different distribution.
      for (int disNum: listTest) {
        int dataSize = DEFAULT_BLOCK_SIZE * disNum;
        // Create ec file.
        Path ecFile = new Path(ecDir, "file" + disNum);
        byte[] expected = StripedFileTestUtil.generateBytes(dataSize);
        int totalNum = disNum + parityBlocks;
        DFSTestUtil.writeFile(fs, ecFile, new String(expected));
        StripedFileTestUtil.waitBlockGroupsReported(fs, ecFile.toString());
        StripedFileTestUtil.verifyLength(fs, ecFile, dataSize);
        assertEquals(ReplicationRule.parseFromMap(ZoneMover.getBlockDistribution(
                DFSTestUtil.getAllBlocks(fs, ecFile).get(0))),
            ReplicationRule.parseFromString("/datacenter0:" + totalNum));
        map.put(totalNum, ecFile);
      }

      // Adding 6 new hosts about '/datacenter1'.
      cluster.startDataNodes(conf, 6, true, null,
          new String[]{"/datacenter1/rack0", "/datacenter1/rack0", "/datacenter1/rack1",
              "/datacenter1/rack1", "/datacenter1/rack2", "/datacenter1/rack2"},
          new String[]{"host6", "host7", "host8", "host9", "host10", "host11"},
          null);
      cluster.triggerBlockReports();
      List<Path> pathList = new ArrayList<>(map.values());
      ZoneMoverWithDRV2.runWithColdDataReplication(conf, cluster.getURI(), pathList,
          false, true, false, false);

      Map<Integer, ReplicationRule> expectedRule = new HashMap<>();
      expectedRule.put(3 + parityBlocks,
          ReplicationRule.parseFromString("/datacenter0:6"));
      expectedRule.put(5 + parityBlocks,
          ReplicationRule.parseFromString("/datacenter0:6,/datacenter1:2"));
      expectedRule.put(6 + parityBlocks,
          ReplicationRule.parseFromString("/datacenter0:6,/datacenter1:3"));

      // Validate striped block rule.
      for (Map.Entry<Integer, Path> entry : map.entrySet()) {
        Integer totalNum = entry.getKey();
        Path path = entry.getValue();
        GenericTestUtils.waitFor(() -> {
          try {
            return expectedRule.get(totalNum).equals(
                ReplicationRule.parseFromMap(ZoneMover.getBlockDistribution(
                    DFSTestUtil.getAllBlocks(fs, path).get(0))));
          } catch (IOException e) {
            return false;
          }
        }, 500, 60000);
      }
    }
  }

  @Test
  public void testZoneMoverWithDRMonitorMode() throws Exception {
    Configuration conf = new HdfsConfiguration();
    final String[] racks = {"/datacenter0/rack1", "/datacenter0/rack2", "/datacenter0/rack3",
        "/datacenter0/rack4", "/datacenter0/rack5", "/datacenter0/rack6"};
    final String[] hosts = {"host0", "host1", "host2", "host3", "host4", "host5"};
    initConfForDr(conf, "/datacenter0", 0);

    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).
        numDataNodes(hosts.length).hosts(hosts).racks(racks).build()) {
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      fs.mkdir(new Path("/test"), new FsPermission("777"));

      final int[] listTest = {2, 3, 4, 5, 6};
      List<Path> paths = new ArrayList<>(Collections.singletonList(new Path("/test")));
      List<Pair<ConsumerRecord<String, String>, String>> records = new ArrayList<>();
      ConsumerRecord<String, String> record;
      // Prepare the files with different distribution.
      for (int disNum: listTest) {
        short replication = (short) disNum;
        Path path = new Path("/test/File." + disNum);
        DFSTestUtil.createFile(fs, path, FILE_LEN, replication, 0L);
        DFSTestUtil.waitReplication(fs, path, replication);
        record = new ConsumerRecord<>("test-topic", 0, disNum, "key1", "value1");
        records.add(Pair.of(record, path.toString()));
      }

      // Adding 6 new hosts about '/datacenter1'.
      cluster.startDataNodes(conf, 6, true, null,
          new String[]{"/datacenter1/rack0", "/datacenter1/rack0", "/datacenter1/rack2",
              "/datacenter1/rack2", "/datacenter1/rack1", "/datacenter1/rack1"},
          new String[]{"host6", "host7", "host8", "host9", "host10", "host11"},
          null);
      cluster.triggerBlockReports();
      ZoneMoverTrigger zoneMoverTrigger = new TestZoneMoverKafkaTrigger(records);
      ZoneMoverWithDRV2 zm = null;
      try {
        zm = new ZoneMoverWithDRV2(conf, cluster.getURI(), paths, RunMode.MONITOR,
            zoneMoverTrigger, false, false, false, false);
        zm.initMoverMetrics();
        ZoneMoverMetrics zoneMoverMetric = zm.getZoneMoverMetrics();
        assertEquals(zoneMoverMetric.getSuccessFiles().lastStat().numSamples(), 0);
        zm.startInTriggerMonitor();

        Map<Short, ReplicationRule> expectedRule = new HashMap<>();
        expectedRule.put((short) 2, ReplicationRule.parseFromString("/datacenter0:1,/datacenter1:1"));
        expectedRule.put((short) 3, ReplicationRule.parseFromString("/datacenter0:2,/datacenter1:1"));
        expectedRule.put((short) 4, ReplicationRule.parseFromString("/datacenter0:2,/datacenter1:2"));
        expectedRule.put((short) 5, ReplicationRule.parseFromString("/datacenter0:3,/datacenter1:2"));
        expectedRule.put((short) 6, ReplicationRule.parseFromString("/datacenter0:3,/datacenter1:3"));

        // Validate replica rule.
        for (Pair<ConsumerRecord<String, String>, String> pair : records) {
          String path = pair.getRight();
          GenericTestUtils.waitFor(() -> {
            try {
              return expectedRule.get(fs.getFileStatus(new Path(path)).getReplication()).equals(
                  ReplicationRule.parseFromMap(ZoneMover.getBlockDistribution(
                      DFSTestUtil.getAllBlocks(fs, new Path(path)).get(0))));
            } catch (IOException e) {
              return false;
            }
          }, 500, 50000);
        }

        GenericTestUtils.waitFor(()->
                zoneMoverMetric.getSuccessFiles().lastStat().numSamples() == 5,
            10, 50000);
      } finally {
        if (zm != null) {
          zm.shutdown();
        }
        zoneMoverTrigger.shutdown();
      }
    }
  }

  @Test
  public void testZoneMoverWithDRByBlacklist() throws Exception {
    GenericTestUtils.LogCapturer logs =
        GenericTestUtils.LogCapturer.captureLogs(ZoneMoverV2.LOG);

    Configuration conf = new HdfsConfiguration();
    final String[] racks = {"/datacenter0/rack0", "/datacenter0/rack1", "/datacenter0/rack2"
        , "/datacenter0/rack3", "/datacenter0/rack4", "/datacenter0/rack5",
        "/datacenter0/rack6", "/datacenter0/rack7", "/datacenter0/rack8"};
    final String[] hosts = {"host0", "host1", "host2", "host10", "host11", "host12",
        "host20", "host21", "host22"};
    initConfForDr(conf, "/datacenter0", 0);
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).
        nnTopology(MiniDFSNNTopology.simpleHATopology()).
        numDataNodes(hosts.length).hosts(hosts).racks(racks).build()) {
      HATestUtil.setFailoverConfigurations(cluster, conf, "dev2");
      cluster.waitActive();
      cluster.transitionToActive(0);
      DistributedFileSystem fs = cluster.getFileSystem(0);

      short replication = 3;
      // create cold mode file.
      Path path = new Path("/test/dir/file");
      createAndVerifyFile(fs, path, replication,
          ReplicationRule.parseFromString("/datacenter0:3"), null);
      Path blacklistPath = new Path("/test/dir/subdir1/file");
      createAndVerifyFile(fs, blacklistPath, replication,
          ReplicationRule.parseFromString("/datacenter0:3"), null);
      Path path1 = new Path("/test1/file");
      createAndVerifyFile(fs, path1, replication,
          ReplicationRule.parseFromString("/datacenter0:3"), null);
      Path blacklistPath1 = new Path("/test2/dir1/file");
      createAndVerifyFile(fs, blacklistPath1, replication,
          ReplicationRule.parseFromString("/datacenter0:3"), null);
      Path path2 = new Path("/test2/dir2/file");
      createAndVerifyFile(fs, path2, replication,
          ReplicationRule.parseFromString("/datacenter0:3"), null);

      //create monitor mode file.
      List<Path> paths = new ArrayList<>(Collections.singletonList(
          new Path("/testMonitor")));
      List<Pair<ConsumerRecord<String, String>, String>> records = new ArrayList<>();
      String blackListPath = "/testMonitor/dir.1";
      Map<String, ReplicationRule> expectedRuleMonitor = new HashMap<>();
      Path monitorPath1 = new Path(blackListPath + "/file.1");
      createAndVerifyFile(fs, monitorPath1, replication,
          ReplicationRule.parseFromString("/datacenter0:3"), records);
      Path monitorPath2 = new Path("/testMonitor/dir.2/file.1");
      createAndVerifyFile(fs, monitorPath2, replication,
          ReplicationRule.parseFromString("/datacenter0:3"), records);

      // Adding 6 new hosts about '/datacenter1'.
      cluster.startDataNodes(conf, 3, true, null,
          new String[]{"/datacenter1/rack0", "/datacenter1/rack1", "/datacenter1/rack2"},
          new String[]{"host6", "host7", "host8"},
          null);
      cluster.triggerBlockReports();
      assertEquals("Number of datanodes should be 12", 12,
          cluster.getDataNodes().size());

      // Validate cold mode skip blacklist.
      ZoneMoverWithDRV2.Cli tool = new ZoneMoverWithDRV2.Cli();
      // Set blacklist path.
      conf.set(DFSConfigKeys.DFS_NAMENODE_DR_BLACKLIST_PATHS, "/test/dir/subdir1,/test2/dir1");
      tool.setConf(conf);
      String[] args = {"-namespace", "dev2", "-path", "/test,/test1,/test2", "-cold"};
      assertEquals(ExitStatus.SUCCESS.getExitCode(), tool.run(args));

      Map<String, ReplicationRule> expectedRule = new HashMap<>();
      expectedRule.put(path.toString(),
          ReplicationRule.parseFromString("/datacenter0:1,/datacenter1:2"));
      expectedRule.put(path1.toString(),
          ReplicationRule.parseFromString("/datacenter0:1,/datacenter1:2"));
      expectedRule.put(path2.toString(),
          ReplicationRule.parseFromString("/datacenter0:1,/datacenter1:2"));

      // blacklistPath and blacklistPath1 will not skip dr.
      expectedRule.put(blacklistPath.toString(),
          ReplicationRule.parseFromString("/datacenter0:3"));
      expectedRule.put(blacklistPath1.toString(),
          ReplicationRule.parseFromString("/datacenter0:3"));

      // Validate replica rule.
      for (Map.Entry<String, ReplicationRule> entry : expectedRule.entrySet()) {
        String file = entry.getKey();
        GenericTestUtils.waitFor(() -> {
          try {
            return expectedRule.get(file).equals(
                ReplicationRule.parseFromMap(ZoneMover.getBlockDistribution(
                    DFSTestUtil.getAllBlocks(fs, new Path(file)).get(0))));
          } catch (IOException e) {
            return false;
          }
        }, 500, 50000);
      }

      assertTrue(logs.getOutput().contains(
          "Skipping blacklisted path /test/dir/subdir1."));
      assertTrue(logs.getOutput().contains(
          "Skipping blacklisted path /test2/dir1."));

      // Validate monitor mode skip blacklist.
      ZoneMoverTrigger zoneMoverTrigger = new TestZoneMoverKafkaTrigger(records);
      conf.set(DFSConfigKeys.DFS_NAMENODE_DR_BLACKLIST_PATHS, blackListPath);
      ZoneMoverWithDRV2 zm = new ZoneMoverWithDRV2(conf, cluster.getURI(0), paths,
          RunMode.MONITOR, zoneMoverTrigger, false, false,
          false, false);
      zm.initMoverMetrics();
      zm.startInTriggerMonitor();

      expectedRuleMonitor.put(monitorPath1.toString(),
          ReplicationRule.parseFromString("/datacenter0:3"));
      expectedRuleMonitor.put(monitorPath2.toString(),
          ReplicationRule.parseFromString("/datacenter0:2,/datacenter1:1"));

      // Validate replica rule.
      for (Map.Entry<String, ReplicationRule> entry : expectedRuleMonitor.entrySet()) {
        String file = entry.getKey();
        GenericTestUtils.waitFor(() -> {
          try {
            return expectedRuleMonitor.get(file).equals(
                ReplicationRule.parseFromMap(ZoneMover.getBlockDistribution(
                    DFSTestUtil.getAllBlocks(fs, new Path(file)).get(0))));
          } catch (IOException e) {
            return false;
          }
        }, 500, 50000);
      }
      assertTrue(logs.getOutput().contains(
          "/testMonitor/dir.1/file.1 will be skipped."));
      zm.shutdown();
      logs.clearOutput();
    }
  }

  private void createAndVerifyFile(DistributedFileSystem fs,
      Path path, short replication, ReplicationRule expectedRule,
      List<Pair<ConsumerRecord<String, String>, String>> records)
      throws IOException, InterruptedException, TimeoutException {
    DFSTestUtil.createFile(fs, path, FILE_LEN, replication, 0L);
    DFSTestUtil.waitReplication(fs, path, replication);
    assertEquals(expectedRule, ReplicationRule.parseFromMap(
        ZoneUtil.getBlockDistribution(DFSTestUtil.getAllBlocks(fs, path).get(0))));
    if (records != null) {
      ConsumerRecord<String, String> record = new ConsumerRecord<>(
          "test-topic", 0, 1, "key1", "value1");
      records.add(Pair.of(record, path.toString()));
    }
  }

  static class TestZoneMoverKafkaTrigger extends ZoneMoverTrigger {
    private final List<Pair<ConsumerRecord<String, String>, String>> pathList;
    private long lastRecordOffset = 0;

    public TestZoneMoverKafkaTrigger(List<Pair<ConsumerRecord<String, String>, String>> pathList) {
      this.pathList = pathList;
    }

    @Override
    public boolean hasNext() {
      return !pathList.isEmpty();
    }

    @Override
    public String getNext() throws InterruptedException {
      return null;
    }

    @Override
    public Pair<ConsumerRecord<String, String>, String> getNextRecord() {
      return pathList.remove(0);
    }

    @Override
    public void updatePaths(List<Path> paths) {
      //nothing;
    }

    @Override
    public void shutdown() {
      //nothing;
    }

    @Override
    public String getGroupId() {
      return "test";
    }

    @Override
    public long saveOffsetToZookeeperCommon(KafkaTopicRecord kafkaTopicRecord) {
      if (kafkaTopicRecord.getOffset() > lastRecordOffset) {
        lastRecordOffset = kafkaTopicRecord.getOffset();
        LOG.info("saveOffset {}", kafkaTopicRecord);
        return 1;
      }
      return -1;
    }

    @Override
    public void saveOffsetToZookeeper(ConsumerRecord<String, String> record, String ns,
        String groupId, ZoneMoverMetrics zoneMoverMetrics) {
      //nothing;
    }

    @Override
    public void saveOffsetToZookeeperForZS(
        ConsumerRecord<String, String> record, String ns, String groupId,
        ZoneServiceMetrics zoneServiceMetrics) {
      // do nothing
    }

    @Override
    public void savePathRecordToZookeeper(String ns, String path) {
      //nothing;
    }

    @Override
    public StoreDriver getStoreDriver() {
      return null;
    }
  }
}
