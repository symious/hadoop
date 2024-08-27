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
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyWithDataCenter;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.hdfs.server.zoneservice.metrics.ZoneMoverMetrics;
import org.apache.hadoop.hdfs.server.zoneservice.metrics.ZoneServiceMetrics;
import org.apache.hadoop.hdfs.server.zoneservice.store.KafkaTopicRecord;
import org.apache.hadoop.hdfs.server.zoneservice.store.StoreDriver;
import org.apache.hadoop.hdfs.server.zoneservice.utils.RunMode;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ZONEMOVER_DISTRIBUTION_RULE_MAP_FILE_KEY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestZoneMigrationV2 {
  private static final long FILE_LEN = 1024;
  private static final int DEFAULT_BLOCK_SIZE = 1024 * 1024;
  private static final ErasureCodingPolicy ecPolicy = SystemErasureCodingPolicies.getByID(
      SystemErasureCodingPolicies.RS_3_2_POLICY_ID);
  private final static String TEST_RULE_MAP_FILE1 = "testRuleMapFile1";

  @BeforeClass
  public static void setLogging() {
  }

  private void initConf(Configuration conf) {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setClass(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
        BlockPlacementPolicyWithDataCenter.class,
        BlockPlacementPolicy.class);
    conf.setBoolean(DFSConfigKeys.DFS_USE_DFS_NETWORK_TOPOLOGY_KEY, true);
    conf.setClass(DFSConfigKeys.DFS_NET_TOPOLOGY_IMPL_KEY,
        DFSNetworkTopologyWithDataCenter.class, DFSNetworkTopology.class);
    conf.set(DFSConfigKeys.DFS_NAMENODE_BLOCK_PLACEMENT_POLICY_WITH_DATA_CENTER_FALLBACK_DC_KEY,
        "/AirTrunk");
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 500);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY, 10);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.set(DFSConfigKeys.DFS_ZONEMOVER_VALID_DATACENTERS_KEY, "/AirTrunk,/STT,/YTL");
  }

  @Test
  public void testZoneMigrationCli() throws Exception {
    Configuration conf = new HdfsConfiguration();
    final String[] racks = {"/AirTrunk/rack0", "/AirTrunk/rack1", "/AirTrunk/rack2"
        , "/AirTrunk/rack3", "/AirTrunk/rack4", "/AirTrunk/rack5"};
    final String[] hosts = {"host0", "host1", "host2", "host10", "host11", "host12"};
    initConf(conf);
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).
        nnTopology(MiniDFSNNTopology.simpleHATopology()).
        numDataNodes(hosts.length).hosts(hosts).racks(racks).build()) {
      HATestUtil.setFailoverConfigurations(cluster, conf, "dev1");
      cluster.waitActive();
      cluster.transitionToActive(0);

      // Spy tool.
      ZoneMigrationV2.Cli tool = Mockito.spy(new ZoneMigrationV2.Cli());


      tool.setConf(conf);

      // Unable to match namespace.
      String[] args = {"-namespace", "dev", "-path", "/test", "-cold"};
      assertEquals(ExitStatus.ILLEGAL_ARGUMENTS.getExitCode(), tool.run(args));

      // Validate must specify either '-rule' or both '-sourceDC' and '-targetDC'.
      String[] args1 = {"-namespace", "dev1", "-path", "/test"};
      assertEquals(ExitStatus.ILLEGAL_ARGUMENTS.getExitCode(), tool.run(args1));

      // Invalid path.
      String[] args2 = {"-namespace", "dev1", "-path", "test", "-sourceDC", "/AirTrunk",
          "-targetDC", "/YTL"};
      assertEquals(ExitStatus.ILLEGAL_ARGUMENTS.getExitCode(), tool.run(args2));

      String ruleMapFile = Objects.requireNonNull(TestMigrationRuleMap.class.getClassLoader()
          .getResource(TEST_RULE_MAP_FILE1)).getPath();
      conf.set(DFS_ZONEMOVER_DISTRIBUTION_RULE_MAP_FILE_KEY, ruleMapFile);

      // Validate correct parameter settings.
      String[] args3 = {"-namespace", "dev1", "-path", "/test", "-sourceDC", "/AirTrunk",
          "-targetDC", "/YTL", "-isDecrease", "-changeReplica"};
      assertEquals(ExitStatus.SUCCESS.getExitCode(), tool.run(args3));

      String[] args33 = {"-namespace", "dev1", "-path", "/test", "-sourceDC", "/AirTrunk",
          "-targetDC", "/YTL"};
      assertEquals(ExitStatus.SUCCESS.getExitCode(), tool.run(args33));

      String[] args333 = {"-namespace", "dev1", "-path", "/test", "-rule", "/AirTrunk:1,/YTL:2"};
      assertEquals(ExitStatus.SUCCESS.getExitCode(), tool.run(args333));

      DistributedFileSystem fs = cluster.getFileSystem(0);
      short replication = 2;
      Path path1 = new Path("/test/File1");
      DFSTestUtil.createFile(fs, path1, FILE_LEN, replication, 0L);
      DFSTestUtil.waitReplication(fs, path1, replication);
      assertEquals(ReplicationRule.parseFromMap(ZoneUtil.getBlockDistribution(
              DFSTestUtil.getAllBlocks(fs, path1).get(0))),
          ReplicationRule.parseFromString("/AirTrunk:2"));

      replication = 3;
      Path path2 = new Path("/test/File2");
      DFSTestUtil.createFile(fs, path2, FILE_LEN, replication, 0L);
      DFSTestUtil.waitReplication(fs, path2, replication);
      assertEquals(ReplicationRule.parseFromMap(ZoneUtil.getBlockDistribution(
              DFSTestUtil.getAllBlocks(fs, path2).get(0))),
          ReplicationRule.parseFromString("/AirTrunk:3"));

      Path path3 = new Path("/test1/File3");
      DFSTestUtil.createFile(fs, path3, FILE_LEN, replication, 0L);
      DFSTestUtil.waitReplication(fs, path3, replication);
      assertEquals(ReplicationRule.parseFromMap(ZoneUtil.getBlockDistribution(
              DFSTestUtil.getAllBlocks(fs, path3).get(0))),
          ReplicationRule.parseFromString("/AirTrunk:3"));

      replication = 5;
      Path path5 = new Path("/test/File5");
      DFSTestUtil.createFile(fs, path5, FILE_LEN, replication, 0L);
      DFSTestUtil.waitReplication(fs, path5, replication);
      assertEquals(ReplicationRule.parseFromMap(ZoneUtil.getBlockDistribution(
              DFSTestUtil.getAllBlocks(fs, path5).get(0))),
          ReplicationRule.parseFromString("/AirTrunk:5"));

      // Create ec dir.
      Path ecDir = new Path("/test/ec");
      fs.mkdirs(ecDir);
      fs.enableErasureCodingPolicy(ecPolicy.getName());
      fs.setErasureCodingPolicy(ecDir, ecPolicy.getName());
      // Create ec file.
      Path ecFile = new Path(ecDir, "file");
      int dataSize = DEFAULT_BLOCK_SIZE * 3;
      byte[] expected = StripedFileTestUtil.generateBytes(dataSize);
      DFSTestUtil.writeFile(fs, ecFile, new String(expected));
      StripedFileTestUtil.waitBlockGroupsReported(fs, ecFile.toString());
      StripedFileTestUtil.verifyLength(fs, ecFile, dataSize);
      assertEquals(ReplicationRule.parseFromMap(ZoneUtil.getBlockDistribution(
              DFSTestUtil.getAllBlocks(fs, ecFile).get(0))),
          ReplicationRule.parseFromString("/AirTrunk:5"));

      // Adding 6 new hosts about '/YTL'.
      cluster.startDataNodes(conf, 6, true, null,
          new String[]{"/YTL/rack0", "/YTL/rack1", "/YTL/rack2",
              "/YTL/rack3", "/STT/rack4", "/YTL/rack5"},
          new String[]{"host6", "host7", "host8", "host9", "host10", "host11"},
          null);
      cluster.triggerBlockReports();
      assertEquals("Number of datanodes should be 12", 12,
          cluster.getDataNodes().size());

      // Validate use "-path" replica rule.
      ruleMapFile = Objects.requireNonNull(TestMigrationRuleMap.class.getClassLoader()
          .getResource(TEST_RULE_MAP_FILE1)).getPath();
      conf.set(DFS_ZONEMOVER_DISTRIBUTION_RULE_MAP_FILE_KEY, ruleMapFile);

      ZoneMigrationV2.Cli toolZoneMigration = new ZoneMigrationV2.Cli();
      toolZoneMigration.setConf(conf);
      String[] args5 = {"-namespace", "dev1", "-path", "/test", "-sourceDC", "/AirTrunk",
          "-targetDC", "/YTL", "-changeReplica"};
      assertEquals(ExitStatus.SUCCESS.getExitCode(), toolZoneMigration.run(args5));

      GenericTestUtils.waitFor(() -> {
        try {
          return ReplicationRule.parseFromMap(
              ZoneUtil.getBlockDistribution(DFSTestUtil.getAllBlocks(fs, path1).get(0))).
              equals(ReplicationRule.parseFromString("/AirTrunk:1,/YTL:1"));
        } catch (IOException e) {
          return false;
        }
      }, 500, 30000);

      GenericTestUtils.waitFor(() -> {
        try {
          return ReplicationRule.parseFromMap(
              ZoneUtil.getBlockDistribution(DFSTestUtil.getAllBlocks(fs, path2).get(0))).
              equals(ReplicationRule.parseFromString("/AirTrunk:1,/YTL:2"));
        } catch (IOException e) {
          return false;
        }
      }, 500, 30000);

      // Validate ec data.
      GenericTestUtils.waitFor(() -> {
        try {
          return ReplicationRule.parseFromMap(
              ZoneUtil.getBlockDistribution(DFSTestUtil.getAllBlocks(fs, ecFile).get(0))).
              equals(ReplicationRule.parseFromString("/YTL:5"));
        } catch (IOException e) {
          return false;
        }
      }, 500, 30000);

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

      GenericTestUtils.waitFor(() -> {
        try {
          return ReplicationRule.parseFromMap(
              ZoneUtil.getBlockDistribution(DFSTestUtil.getAllBlocks(fs, path5).get(0))).
              equals(ReplicationRule.parseFromString("/AirTrunk:2,/YTL:3"));
        } catch (IOException e) {
          return false;
        }
      }, 500, 30000);

      // Adding 3 new hosts about '/STT'.
      cluster.startDataNodes(conf, 3, true, null,
          new String[]{"/STT/rack0", "/STT/rack1", "/STT/rack2"},
          new String[]{"host22", "host23", "host21"},
          null);
      cluster.triggerBlockReports();
      assertEquals("Number of datanodes should be 15", 15,
          cluster.getDataNodes().size());

      String[] args6 = {"-namespace", "dev1", "-path", path3.toString(), "-rule",
          "/AirTrunk:2,/STT:1"};
      assertEquals(ExitStatus.SUCCESS.getExitCode(), toolZoneMigration.run(args6));

      GenericTestUtils.waitFor(() -> {
        try {
          return ReplicationRule.parseFromMap(
              ZoneUtil.getBlockDistribution(DFSTestUtil.getAllBlocks(fs, path3).get(0))).
              equals(ReplicationRule.parseFromString("/AirTrunk:2,/STT:1"));
        } catch (IOException e) {
          return false;
        }
      }, 500, 30000);

      String[] args7 = {"-namespace", "dev1", "-path", "/test1", "-sourceDC", "/AirTrunk",
          "-targetDC", "/YTL", "-changeReplica"};
      assertEquals(ExitStatus.SUCCESS.getExitCode(), toolZoneMigration.run(args7));

      GenericTestUtils.waitFor(() -> {
        try {
          return ReplicationRule.parseFromMap(
              ZoneUtil.getBlockDistribution(DFSTestUtil.getAllBlocks(fs, path3).get(0))).
              equals(ReplicationRule.parseFromString("/AirTrunk:1,/STT:1,/YTL:2"));
        } catch (IOException e) {
          return false;
        }
      }, 500, 30000);
    }
  }

  @Test
  public void testZonMigrationRMonitorMode() throws Exception {
    Configuration conf = new HdfsConfiguration();
    final String[] racks = {"/AirTrunk/rack0", "/AirTrunk/rack1", "/AirTrunk/rack2"
        , "/AirTrunk/rack3", "/AirTrunk/rack4", "/AirTrunk/rack5"};
    final String[] hosts = {"host0", "host1", "host2", "host10", "host11", "host12"};
    initConf(conf);
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

      // Adding 6 new hosts about '/YTL'.
      cluster.startDataNodes(conf, 6, true, null,
          new String[]{"/YTL/rack0", "/YTL/rack1", "/YTL/rack2",
              "/YTL/rack3", "/STT/rack4", "/YTL/rack5"},
          new String[]{"host6", "host7", "host8", "host9", "host10", "host11"},
          null);
      cluster.triggerBlockReports();

      String ruleMapFile = Objects.requireNonNull(TestMigrationRuleMap.class.getClassLoader()
          .getResource(TEST_RULE_MAP_FILE1)).getPath();
      conf.set(DFS_ZONEMOVER_DISTRIBUTION_RULE_MAP_FILE_KEY, ruleMapFile);
      ZoneMigrationV2 zm = null;
      ZoneMoverTrigger zoneMoverTrigger = new TestZoneMigrationV2.TestZoneMoverKafkaTrigger(records);
      try {
        zm = new ZoneMigrationV2(conf,  cluster.getURI(), paths, RunMode.MONITOR,
            zoneMoverTrigger, null,
            true, "/AirTrunk", "/YTL", false);
        zm.initMoverMetrics();
        ZoneMoverMetrics zoneMoverMetric = zm.getZoneMoverMetrics();
        assertEquals(zoneMoverMetric.getSuccessFiles().lastStat().numSamples(), 0);
        zm.startInTriggerMonitor();

        Map<Short, ReplicationRule> expectedRule = new HashMap<>();
        expectedRule.put((short) 2, ReplicationRule.parseFromString("/AirTrunk:1,/YTL:1"));
        expectedRule.put((short) 3, ReplicationRule.parseFromString("/AirTrunk:1,/YTL:2"));
        expectedRule.put((short) 4, ReplicationRule.parseFromString("/AirTrunk:2,/YTL:2"));
        expectedRule.put((short) 5, ReplicationRule.parseFromString("/AirTrunk:2,/YTL:3"));
        expectedRule.put((short) 6, ReplicationRule.parseFromString("/AirTrunk:3,/YTL:3"));

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
            10, 20000);
      } finally {
        if (zm != null) {
          zm.shutdown();
        }
        zoneMoverTrigger.shutdown();
      }
    }
  }

  static class TestZoneMoverKafkaTrigger extends ZoneMoverTrigger {
    List<Pair<ConsumerRecord<String, String>, String>> pathList;

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
