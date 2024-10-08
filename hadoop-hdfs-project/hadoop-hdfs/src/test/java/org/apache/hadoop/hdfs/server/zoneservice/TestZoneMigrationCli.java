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

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.hdfs.server.zoneservice.metrics.ZoneProgressTracker;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ZONEMOVER_DISTRIBUTION_RULE_MAP_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ZONE_SUPPORT_MIGRATE_REPLICA_RULES_KEY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestZoneMigrationCli {
  private static final long FILE_LEN = 1024;
  private static final int DEFAULT_BLOCK_SIZE = 1024 * 1024;
  private static final ErasureCodingPolicy ecPolicy = SystemErasureCodingPolicies.getByID(
      SystemErasureCodingPolicies.RS_3_2_POLICY_ID);
  private final static String TEST_RULE_MAP_FILE1 = "testRuleMapFile1";
  private static HdfsConfiguration conf;
  private static MiniDFSCluster cluster;
  private static ZoneMigration.Cli tool;
  private static DistributedFileSystem fs;
  private static Path path1 = new Path("/test/File");
  private static Path path2 = new Path("/test/File1");
  private static Path path3 = new Path("/test1/File3");
  private static Path path5 = new Path("/test/File5");

  @BeforeClass
  public static void setLogging() throws IOException, InterruptedException {
    LogManager.getLogger(ZoneProgressTracker.class.getName()).setLevel(Level.DEBUG);
    LogManager.getLogger(ZoneMigration.class.getName()).setLevel(Level.DEBUG);
    GenericTestUtils.setLogLevel(BlockManager.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(NameNode.blockStateChangeLog, Level.DEBUG);
    GenericTestUtils.setLogLevel(ZoneMigration.LOG, Level.DEBUG);

    conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY, false);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setBoolean(DFSConfigKeys.DFS_USE_DFS_NETWORK_TOPOLOGY_KEY, true);
    conf.setClass(DFSConfigKeys.DFS_NET_TOPOLOGY_IMPL_KEY,
        DFSNetworkTopologyWithDataCenter.class, DFSNetworkTopology.class);
    conf.set(DFSConfigKeys.DFS_NAMENODE_BLOCK_PLACEMENT_POLICY_WITH_DATA_CENTER_FALLBACK_DC_KEY,
        "/AirTrunk");
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 500);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY, 10);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.set(DFSConfigKeys.DFS_ZONEMOVER_VALID_DATACENTERS_KEY, "/AirTrunk,/STT,/YTL");
    conf.set(DFS_ZONE_SUPPORT_MIGRATE_REPLICA_RULES_KEY,
        "2=/AirTrunk:1,/YTL:1,/STT:1;3=/AirTrunk:1,/YTL:2,/STT:1");

    final String[] racks = {"/AirTrunk/rack0", "/AirTrunk/rack1", "/AirTrunk/rack2"
        , "/AirTrunk/rack3", "/AirTrunk/rack4", "/AirTrunk/rack5"};
    final String[] hosts = {"host0", "host1", "host2", "host10", "host11", "host12"};
    cluster = new MiniDFSCluster.Builder(conf).
        nnTopology(MiniDFSNNTopology.simpleHATopology()).
        numDataNodes(hosts.length).hosts(hosts).racks(racks).build();
    HATestUtil.setFailoverConfigurations(cluster, conf, "dev1");
    cluster.waitActive();
    cluster.transitionToActive(0);
    fs = cluster.getFileSystem(0);

    // Spy tool.
    tool = new SpiedCli();
    tool.setConf(conf);
  }

  static class SpiedCli extends ZoneMigration.Cli {
    int run(Configuration conf, URI namenode, List<Path> paths, ReplicationRule rule,
        String sourceDC, String targetDC, boolean isDecrease, boolean changeReplica) {
      return ExitStatus.SUCCESS.getExitCode();
    }
  }

  @AfterClass
  public static void cleanup() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testZoneMigrationCli1() {
    // Unable to match namespace.
    String[] args = { "-namespace", "dev", "-path", "/test", "-cold" };
    assertEquals(ExitStatus.ILLEGAL_ARGUMENTS.getExitCode(), tool.run(args));

    // Validate must specify either '-rule' or both '-sourceDC' and '-targetDC'.
    String[] args1 = { "-namespace", "dev1", "-path", "/test" };
    assertEquals(ExitStatus.ILLEGAL_ARGUMENTS.getExitCode(), tool.run(args1));

    // Invalid path.
    String[] args2 =
        { "-namespace", "dev1", "-path", "test", "-sourceDC", "/AirTrunk", "-targetDC", "/YTL" };
    assertEquals(ExitStatus.ILLEGAL_ARGUMENTS.getExitCode(), tool.run(args2));
  }

  @Test
  public void testZoneMigrationCli2() {
    String ruleMapFile = Objects.requireNonNull(
        TestMigrationRuleMap.class.getClassLoader().getResource(TEST_RULE_MAP_FILE1)).getPath();
    conf.set(DFS_ZONEMOVER_DISTRIBUTION_RULE_MAP_FILE_KEY, ruleMapFile);

    // Validate correct parameter settings.
    String[] args3 =
        { "-namespace", "dev1", "-path", "/test", "-sourceDC", "/AirTrunk", "-targetDC", "/YTL",
            "-isDecrease", "-changeReplica" };
    assertEquals(ExitStatus.SUCCESS.getExitCode(), tool.run(args3));

    String[] args33 =
        { "-namespace", "dev1", "-path", "/test", "-sourceDC", "/AirTrunk", "-targetDC", "/YTL" };
    assertEquals(ExitStatus.SUCCESS.getExitCode(), tool.run(args33));

    String[] args333 = { "-namespace", "dev1", "-path", "/test", "-rule", "/AirTrunk:1,/YTL:2" };
    assertEquals(ExitStatus.SUCCESS.getExitCode(), tool.run(args333));
  }

//  @Test(timeout = 300000L)
  public void testZoneMigrationCli3() throws Exception {
    short replication = 2;
    DFSTestUtil.createFile(fs, path1, FILE_LEN, replication, 0L);
    DFSTestUtil.waitReplication(fs, path1, replication);
    assertEquals(ReplicationRule.parseFromMap(
            ZoneMover.getBlockDistribution(DFSTestUtil.getAllBlocks(fs, path1).get(0))),
        ReplicationRule.parseFromString("/AirTrunk:2"));

    replication = 3;
    DFSTestUtil.createFile(fs, path2, FILE_LEN, replication, 0L);
    DFSTestUtil.waitReplication(fs, path2, replication);
    assertEquals(ReplicationRule.parseFromMap(
            ZoneMover.getBlockDistribution(DFSTestUtil.getAllBlocks(fs, path2).get(0))),
        ReplicationRule.parseFromString("/AirTrunk:3"));

    DFSTestUtil.createFile(fs, path3, FILE_LEN, replication, 0L);
    DFSTestUtil.waitReplication(fs, path3, replication);
    assertEquals(ReplicationRule.parseFromMap(
            ZoneMover.getBlockDistribution(DFSTestUtil.getAllBlocks(fs, path3).get(0))),
        ReplicationRule.parseFromString("/AirTrunk:3"));

    replication = 5;
    DFSTestUtil.createFile(fs, path5, FILE_LEN, replication, 0L);
    DFSTestUtil.waitReplication(fs, path5, replication);
    assertEquals(ReplicationRule.parseFromMap(
            ZoneMover.getBlockDistribution(DFSTestUtil.getAllBlocks(fs, path5).get(0))),
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
    assertEquals(ReplicationRule.parseFromMap(
            ZoneMover.getBlockDistribution(DFSTestUtil.getAllBlocks(fs, ecFile).get(0))),
        ReplicationRule.parseFromString("/AirTrunk:5"));

    // Adding 6 new hosts about '/YTL'.
    cluster.startDataNodes(conf, 6, true, null,
        new String[] { "/YTL/rack0", "/YTL/rack1", "/YTL/rack2", "/YTL/rack3", "/STT/rack4",
            "/YTL/rack5" }, new String[] { "host6", "host7", "host8", "host9", "host10", "host11" },
        null);
    cluster.triggerBlockReports();
    assertEquals("Number of datanodes should be 12", 12, cluster.getDataNodes().size());

    // Validate use "-path" replica rule.
    String ruleMapFile = Objects.requireNonNull(
        TestMigrationRuleMap.class.getClassLoader().getResource(TEST_RULE_MAP_FILE1)).getPath();
    conf.set(DFS_ZONEMOVER_DISTRIBUTION_RULE_MAP_FILE_KEY, ruleMapFile);

    ZoneMigration.Cli toolZoneMigration = new ZoneMigration.Cli();
    toolZoneMigration.setConf(conf);
    String[] args5 =
        { "-namespace", "dev1", "-path", "/test", "-sourceDC", "/AirTrunk", "-targetDC", "/YTL",
            "-changeReplica" };
    assertEquals(ExitStatus.SUCCESS.getExitCode(), toolZoneMigration.run(args5));

    GenericTestUtils.waitFor(() -> {
      try {
        return ReplicationRule.parseFromMap(
                ZoneMover.getBlockDistribution(DFSTestUtil.getAllBlocks(fs, path1).get(0)))
            .equals(ReplicationRule.parseFromString("/AirTrunk:1,/YTL:1"));
      } catch (IOException e) {
        return false;
      }
    }, 500, 30000);

    GenericTestUtils.waitFor(() -> {
      try {
        return ReplicationRule.parseFromMap(
                ZoneMover.getBlockDistribution(DFSTestUtil.getAllBlocks(fs, path2).get(0)))
            .equals(ReplicationRule.parseFromString("/AirTrunk:1,/YTL:2"));
      } catch (IOException e) {
        return false;
      }
    }, 500, 30000);

    // Validate ec data.
    GenericTestUtils.waitFor(() -> {
      try {
        return ReplicationRule.parseFromMap(
                ZoneMover.getBlockDistribution(DFSTestUtil.getAllBlocks(fs, ecFile).get(0)))
            .equals(ReplicationRule.parseFromString("/YTL:5"));
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
        return ReplicationRule.parseFromMap(ZoneMover.getBlockDistribution(
                DFSTestUtil.getAllBlocks(fs, path5).get(0))).
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
        return ReplicationRule.parseFromMap(ZoneMover.getBlockDistribution(
                DFSTestUtil.getAllBlocks(fs, path3).get(0))).
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
        return ReplicationRule.parseFromMap(ZoneMover.getBlockDistribution(
                DFSTestUtil.getAllBlocks(fs, path3).get(0))).
            equals(ReplicationRule.parseFromString("/AirTrunk:1,/STT:1,/YTL:2"));
      } catch (IOException e) {
        return false;
      }
    }, 500, 30000);
  }
}
