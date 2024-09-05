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
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyDefault;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyWithDataCenter;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyWithDefaultFallbackDataCenter;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.hdfs.server.zoneservice.metrics.ZoneMoverMetrics;
import org.apache.hadoop.hdfs.server.zoneservice.metrics.ZoneProgressTracker;
import org.apache.hadoop.hdfs.server.zoneservice.metrics.ZoneServiceMetrics;
import org.apache.hadoop.hdfs.server.zoneservice.store.KafkaTopicRecord;
import org.apache.hadoop.hdfs.server.zoneservice.store.PathRecord;
import org.apache.hadoop.hdfs.server.zoneservice.store.StoreDriver;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestZoneMoverWithDR {
  private static final long FILE_LEN = 1024;
  private static final int DEFAULT_BLOCK_SIZE = 1024 * 1024;
  private static final ErasureCodingPolicy ecPolicy = StripedFileTestUtil.getDefaultECPolicy();
  private static final short parityBlocks = (short) ecPolicy.getNumParityUnits();

  @BeforeClass
  public static void setLogging() {
    LogManager.getLogger(ZoneProgressTracker.class.getName()).setLevel(Level.DEBUG);
    LogManager.getLogger(ZoneMoverWithDR.class.getName()).setLevel(Level.DEBUG);
  }

  private void initConfForDr(Configuration conf, String defaultDataCenter,
      long drColdDataThresholdMS) {
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
    conf.set(DFSConfigKeys.DFS_NAMENODE_DR_REPLICATION_RULE_COLD_DATA_KEY,
        "3=/datacenter0:1,/datacenter1:2");
    conf.set(DFSConfigKeys.DFS_NAMENODE_DR_STRIPED_BLOCK_RULE_KEY,
        "9=/datacenter0:6,/datacenter1:3");
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_DR_COLD_DATA_THRESHOLD_MS_KEY,
        drColdDataThresholdMS);
  }

  @Test
  public void testZoneMoverWithDRCli() throws Exception {
    GenericTestUtils.setLogLevel(BlockManager.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(NameNode.blockStateChangeLog, Level.DEBUG);
    GenericTestUtils.LogCapturer logs =
        GenericTestUtils.LogCapturer.captureLogs(ZoneMoverWithDR.LOG);

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
      ZoneMoverWithDR.Cli tool = Mockito.spy(new ZoneMoverWithDR.Cli());
      Mockito.doReturn(ExitStatus.SUCCESS.getExitCode()).when(tool).run(
          Mockito.any(Configuration.class),
          Mockito.any(URI.class),
          Mockito.anyListOf(Path.class),
          Mockito.any(Boolean.class),
          Mockito.any(Boolean.class),
          Mockito.any(Boolean.class),
          Mockito.any(Boolean.class));

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
      assertEquals(ReplicationRule.parseFromMap(ZoneMover.getBlockDistribution(
              DFSTestUtil.getAllBlocks(fs, path1).get(0))),
          ReplicationRule.parseFromString("/datacenter0:2"));

      replication = 3;
      Path path2 = new Path("/test2/coldFile");
      DFSTestUtil.createFile(fs, path2, FILE_LEN, replication, 0L);
      DFSTestUtil.waitReplication(fs, path2, replication);
      assertEquals(ReplicationRule.parseFromMap(ZoneMover.getBlockDistribution(
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
      assertEquals(ReplicationRule.parseFromMap(ZoneMover.getBlockDistribution(
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
      ZoneMoverWithDR.Cli tool1 = new ZoneMoverWithDR.Cli();
      conf.setClass(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
          BlockPlacementPolicyDefault.class,
          BlockPlacementPolicy.class);
      tool1.setConf(conf);
      String[] args3 = {"-namespace", "dev1", "-path", "/test2", "-cold", "-useAccessTime"};
      assertEquals(ExitStatus.SUCCESS.getExitCode(), tool1.run(args3));

      GenericTestUtils.waitFor(() -> {
        try {
          return ReplicationRule.parseFromMap(ZoneMover.getBlockDistribution(
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
          return ReplicationRule.parseFromMap(ZoneMover.getBlockDistribution(
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
          return ReplicationRule.parseFromMap(ZoneMover.getBlockDistribution(
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
      for (int disNum: listTest) {
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

      conf.setClass(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
          BlockPlacementPolicyDefault.class,
          BlockPlacementPolicy.class);
      ZoneMoverWithDR.runWithColdDataReplication(conf, cluster.getURI(), pathList,
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

      conf.setClass(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
          BlockPlacementPolicyDefault.class,
          BlockPlacementPolicy.class);
      ZoneMoverWithDR.runWithColdDataReplication(conf, cluster.getURI(), pathList,
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
    final String[] racks = {"/datacenter0/rack0", "/datacenter0/rack0", "/datacenter0/rack2",
        "/datacenter0/rack2", "/datacenter0/rack1", "/datacenter0/rack1"};
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
      ConsumerRecord<String, String> record =
          new ConsumerRecord<>("test-topic", 0, 0, "key1", "value1");
      // Prepare the files with different distribution.
      for (int disNum: listTest) {
        short replication = (short) disNum;
        Path path = new Path("/test/File." + disNum);
        DFSTestUtil.createFile(fs, path, FILE_LEN, replication, 0L);
        DFSTestUtil.waitReplication(fs, path, replication);
        records.add(Pair.of(record, path.toString()));
      }

      // Adding 6 new hosts about '/datacenter1'.
      cluster.startDataNodes(conf, 6, true, null,
          new String[]{"/datacenter1/rack0", "/datacenter1/rack0", "/datacenter1/rack2",
              "/datacenter1/rack2", "/datacenter1/rack1", "/datacenter1/rack1"},
          new String[]{"host6", "host7", "host8", "host9", "host10", "host11"},
          null);
      cluster.triggerBlockReports();

      ZoneMoverMetrics zoneMoverMetric = ZoneMoverWithDR.zoneMoverMetrics;
      assertEquals(zoneMoverMetric.getSuccessTotalMove().lastStat().numSamples(), 0);

      ZoneMoverTrigger zoneMoverTrigger = new TestZoneMoverKafkaTrigger(records);
      conf.setClass(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
          BlockPlacementPolicyDefault.class,
          BlockPlacementPolicy.class);
      ZoneMoverWithDR.runWithNewDataReplication(zoneMoverTrigger, conf, cluster.getURI(), paths,
          false, false);

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
      assertEquals(zoneMoverMetric.getSuccessTotalMove().lastStat().numSamples(), 5);
    }
  }

  static class TestZoneMoverKafkaTrigger extends ZoneMoverTrigger {
    List<Pair<ConsumerRecord<String, String>, String>> pathList;

    public TestZoneMoverKafkaTrigger(List<Pair<ConsumerRecord<String, String>, String>> pathList) {
      this.pathList = pathList;
    }

    @Override
    public boolean hasNext() {
      return pathList.size() > 0;
    }

    @Override
    public String getNext() throws InterruptedException {
      return null;
    }

    @Override
    public Pair<ConsumerRecord<String, String>, String> getNextRecord()
        throws InterruptedException {
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
      return -1;
    }

    @Override
    public void saveOffsetToZookeeper(ConsumerRecord<String, String> record, String ns,
        String groupId, ZoneMoverMetrics zoneMoverMetrics) {
      //nothing;
    }

    @Override
    public void saveOffsetToZookeeperForZS(ConsumerRecord<String, String> record, String ns,
        String groupId, ZoneServiceMetrics zoneServiceMetrics) {
      //nothing;
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
