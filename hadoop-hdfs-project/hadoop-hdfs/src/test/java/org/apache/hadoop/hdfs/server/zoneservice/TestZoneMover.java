package org.apache.hadoop.hdfs.server.zoneservice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.net.DFSNetworkTopology;
import org.apache.hadoop.hdfs.net.DFSNetworkTopologyWithDataCenter;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyWithDataCenter;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.net.StaticMapping;
import org.apache.hadoop.util.Tool;
import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class TestZoneMover {
  
  private MiniDFSCluster cluster = null;
  private static final long FILE_LEN = 1024;
  private static final short REPLICATION = 3;
  private static final Logger LOG =
      LoggerFactory.getLogger(TestZoneMover.class);
  private static final long DFS_HEARTBEAT_INTERVAL = 2;
  
  private Configuration getConf() {
    Configuration conf = new HdfsConfiguration();
    conf.setClass(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
        BlockPlacementPolicyWithDataCenter.class,
        BlockPlacementPolicy.class);
    conf.setBoolean(DFSConfigKeys.DFS_USE_DFS_NETWORK_TOPOLOGY_KEY, true);
    conf.setClass(DFSConfigKeys.DFS_NET_TOPOLOGY_IMPL_KEY,
        DFSNetworkTopologyWithDataCenter.class,
        DFSNetworkTopology.class);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
        DFS_HEARTBEAT_INTERVAL);
    conf.setBoolean(CommonConfigurationKeys.IGNORE_SDI_AUTHENTICATE_KEY, true);
    return conf;
  }
  
  @Test
  public void testZoneMoverCli() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(CommonConfigurationKeys.IGNORE_SDI_AUTHENTICATE_KEY, true);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();

    // Spy tool
    ZoneMover.Cli tool = Mockito.spy(new ZoneMover.Cli());
    Mockito.doReturn(ExitStatus.SUCCESS.getExitCode()).when(tool).run(
        Mockito.any(Configuration.class),
        Mockito.any(URI.class),
        Mockito.anyListOf(Path.class),
        Mockito.any(ReplicationRule.class));
    tool.setConf(cluster.getConfiguration(0));

    // Wrong block placement policy
    String[] args = {"-namespace", "dev",
        "-path", "/test", "-rule", "/sg_dc:3"};
    assertEquals(ExitStatus.IO_EXCEPTION.getExitCode(), tool.run(args));
    
    // Unable to match namespace
    cluster = new MiniDFSCluster.Builder(getConf()).numDataNodes(0).build();
    tool.setConf(cluster.getConfiguration(0));
    assertEquals(ExitStatus.ILLEGAL_ARGUMENTS.getExitCode(), tool.run(args));
    
    // Use the default namespace
    String[] args2 = {"-path", "/test", "-rule", "/sg_dc:3"};
    assertEquals(ExitStatus.SUCCESS.getExitCode(), tool.run(args2));

    // Invalid path
    String[] args3 = {"-path", "test", "-rule", "/sg_dc:3"};
    assertEquals(ExitStatus.ILLEGAL_ARGUMENTS.getExitCode(), tool.run(args3));
  }

  @Test
  public void testZoneMoverCliWithHAConf() throws Exception {
    Configuration conf = getConf();
    cluster = new MiniDFSCluster
        .Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(0).build();
    cluster.waitActive();
    HATestUtil.setFailoverConfigurations(cluster, conf, "dev");

    // Spy tool
    ZoneMover.Cli tool = Mockito.spy(new ZoneMover.Cli());
    Mockito.doReturn(ExitStatus.SUCCESS.getExitCode()).when(tool).run(
        Mockito.any(Configuration.class),
        Mockito.any(URI.class),
        Mockito.anyListOf(Path.class),
        Mockito.any(ReplicationRule.class));
    tool.setConf(conf);
    
    String[] args = {"-namespace", "dev",
        "-path", "/test", "-rule", "/sg_dc:3"};
    assertEquals(ExitStatus.SUCCESS.getExitCode(), tool.run(args));

    // Unable to match namespace
    String[] args2 = {"-namespace", "dev2",
        "-path", "/test", "-rule", "/sg_dc:3"};
    assertEquals(ExitStatus.ILLEGAL_ARGUMENTS.getExitCode(), tool.run(args2));
    
    // Use the default namespace 
    String[] args3 = {"-path", "/test", "-rule", "/sg_dc:3"};
    assertEquals(ExitStatus.SUCCESS.getExitCode(), tool.run(args3));
  }

  @Test
  public void testZoneMover() throws Exception {
    Configuration conf = getConf();
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, REPLICATION);
    cluster = new MiniDFSCluster
        .Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(1).build();
    cluster.waitActive();
    cluster.transitionToActive(0);
    HATestUtil.setFailoverConfigurations(cluster, conf, "dev");
    DistributedFileSystem fs = cluster.getFileSystem(0);
    fs.mkdir(new Path("/test"), new FsPermission("777"));
    fs.mkdir(new Path("/test/foo"), new FsPermission("777"));
    DFSTestUtil.createFile(fs, new Path("/test/test.txt"),
        FILE_LEN, REPLICATION, 0L);

    Tool tool = new ZoneMover.Cli();
    tool.setConf(conf);

    // No block moved as the file is empty
    String[] args = {"-namespace", "dev",
        "-path", "/test", "-rule", "/sg_dc:3"};
    assertEquals(ExitStatus.NO_MOVE_BLOCK.getExitCode(), tool.run(args));
  }

  @Test
  public void testGetBlockDistribution() throws IOException {
    final String[] racks = {"/dc0/rack0", "/dc1/rack1", "/dc1/rack2"};
    Map<String, Short> distribution = new HashMap<>();
    distribution.put("/dc0", (short) 1);
    distribution.put("/dc1", (short) 2);

    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, REPLICATION);
    conf.setBoolean(CommonConfigurationKeys.IGNORE_SDI_AUTHENTICATE_KEY, true);
    cluster = new MiniDFSCluster
        .Builder(conf)
        .numDataNodes(racks.length).racks(racks).build();
    cluster.waitActive();
    DistributedFileSystem fs = cluster.getFileSystem();
    Path path = new Path("/test.txt");
    DFSTestUtil.createFile(fs, path, FILE_LEN, REPLICATION, 0L);
    List<LocatedBlock> blocks = DFSTestUtil.getAllBlocks(fs, path);
    assertEquals(1, blocks.size());
    assertEquals(distribution, ZoneMover.getBlockDistribution(blocks.get(0)));
  }

  @Test
  public void testIsBlockSatisfyRule() throws IOException {
    final String[] racks = {"/dc0/rack0", "/dc1/rack1", "/dc1/rack2"};
    Map<String, Short> distribution = new HashMap<>();
    distribution.put("/dc0", (short) 1);
    distribution.put("/dc1", (short) 2);
    ReplicationRule rule = ReplicationRule.parseFromMap(distribution);

    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, REPLICATION);
    conf.setBoolean(CommonConfigurationKeys.IGNORE_SDI_AUTHENTICATE_KEY, true);
    cluster = new MiniDFSCluster
        .Builder(conf)
        .numDataNodes(racks.length).racks(racks).build();
    cluster.waitActive();

    // construct NameNodeConnector and ZoneMover
    URI namenode = DFSUtil.createUri(HdfsConstants.HDFS_URI_SCHEME,
        cluster.getNameNode().getNameNodeAddress());
    NameNodeConnector nnc = new NameNodeConnector(
        "testIsBlockSatisfyRule",
        namenode,
        new Path("/testIsBlockSatisfyRule"),
        new ArrayList<Path>(), conf, 1);
    ZoneMover zoneMover = new ZoneMover(nnc, conf, rule, new AtomicInteger(1));

    DistributedFileSystem fs = cluster.getFileSystem();
    Path path1 = new Path("/test.txt");
    DFSTestUtil.createFile(fs, path1, FILE_LEN, REPLICATION, 0L);
    List<LocatedBlock> blocks1 = DFSTestUtil.getAllBlocks(fs, path1);
    assertEquals(1, blocks1.size());
    assertTrue(zoneMover.isBlockSatisfyRule(blocks1.get(0)));

    Path path2 = new Path("/test.txt");
    DFSTestUtil.createFile(fs, path2, FILE_LEN, (short) (REPLICATION - 1), 0L);
    List<LocatedBlock> blocks2 = DFSTestUtil.getAllBlocks(fs, path2);
    assertEquals(1, blocks2.size());
    assertFalse(zoneMover.isBlockSatisfyRule(blocks2.get(0)));

  }

  @Test
  public void testGetZoneMoveItems() throws IOException {
    final String[] racks = {"/dc1/rack0", "/dc1/rack1", "/dc1/rack2"};
    Map<String, Short> ruleMap = new HashMap<>();
    ruleMap.put("/dc0", (short) 1);
    ruleMap.put("/dc1", (short) 2);
    ReplicationRule rule = ReplicationRule.parseFromMap(ruleMap);

    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, REPLICATION);
    conf.setBoolean(CommonConfigurationKeys.IGNORE_SDI_AUTHENTICATE_KEY, true);
    cluster = new MiniDFSCluster
        .Builder(conf)
        .numDataNodes(racks.length).racks(racks).build();
    cluster.waitActive();

    // construct NameNodeConnector and ZoneMover
    URI namenode = DFSUtil.createUri(HdfsConstants.HDFS_URI_SCHEME,
        cluster.getNameNode().getNameNodeAddress());
    NameNodeConnector nnc = new NameNodeConnector(
        "testIsBlockSatisfyRule",
        namenode,
        new Path("/testIsBlockSatisfyRule"),
        new ArrayList<Path>(), conf, 1);
    ZoneMover zoneMover = new ZoneMover(nnc, conf, rule, new AtomicInteger(1));

    DistributedFileSystem fs = cluster.getFileSystem();
    Path path1 = new Path("/test.txt");
    DFSTestUtil.createFile(fs, path1, FILE_LEN, REPLICATION, 0L);
    List<LocatedBlock> blocks1 = DFSTestUtil.getAllBlocks(fs, path1);
    assertEquals(1, blocks1.size());
    assertFalse(zoneMover.isBlockSatisfyRule(blocks1.get(0)));

    ZoneMover.ZoneMoveItem moveItem =
        new ZoneMover.ZoneMoveItem("/dc1", "/dc0", (short) 1);
    List<ZoneMover.ZoneMoveItem> items = zoneMover.getZoneMoveItems(blocks1.get(0));
    assertEquals(1, items.size());
    assertEquals(moveItem, items.get(0));
  }

  @Test
  public void testAreBlocksDistributionConsistent() throws IOException {
    final String[] racks = {"/dc0/rack0", "/dc1/rack1", "/dc1/rack2"};
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, REPLICATION);
    conf.setBoolean(CommonConfigurationKeys.IGNORE_SDI_AUTHENTICATE_KEY, true);
    cluster = new MiniDFSCluster
        .Builder(conf)
        .numDataNodes(racks.length).racks(racks).build();
    cluster.waitActive();
    DistributedFileSystem fs = cluster.getFileSystem();

    Path path1 = new Path("/test1.txt");
    DFSTestUtil.createFile(fs, path1, FILE_LEN, REPLICATION, 0L);
    List<LocatedBlock> blocks1 = DFSTestUtil.getAllBlocks(fs, path1);
    Path path2 = new Path("/test2.txt");
    DFSTestUtil.createFile(fs, path2, FILE_LEN, REPLICATION, 0L);
    List<LocatedBlock> blocks2 = DFSTestUtil.getAllBlocks(fs, path2);
    assertTrue(ZoneMover.areBlocksDistributionConsistent(
        Arrays.asList(blocks1.get(0), blocks2.get(0))));

    Path path3 = new Path("/test3.txt");
    DFSTestUtil.createFile(fs, path3, FILE_LEN, (short) (REPLICATION - 1), 0L);
    List<LocatedBlock> blocks3 = DFSTestUtil.getAllBlocks(fs, path3);
    assertFalse(ZoneMover.areBlocksDistributionConsistent(
        Arrays.asList(blocks1.get(0), blocks3.get(0))));
  }

  @Test
  public void testBlockMove() throws Exception {
    // construct a cluster with two datacenters
    StaticMapping.resetMap();
    final String[] hosts1 = {"host0", "host1", "host2"};
    final String[] racks1 = {"/dc0/rack0", "/dc0/rack0", "/dc0/rack1"};
    Configuration conf = getConf();
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, REPLICATION);
    cluster = new MiniDFSCluster
        .Builder(conf)
        .numDataNodes(hosts1.length).hosts(hosts1).racks(racks1).build();
    cluster.waitActive();
    DistributedFileSystem fs = cluster.getFileSystem();
    fs.mkdir(new Path("/test"), new FsPermission("777"));

    // write a file
    Path path = new Path("/test/testBlockMove.txt");
    // client(127.0.0.1) will be mapped to a random node in (host0, host1, host2)
    DFSTestUtil.createFile(fs, path, FILE_LEN, REPLICATION, 0L);

    // validate replica distribution before moving
    Map<String, Short> distribution = new HashMap<>();
    distribution.put("/dc0", (short) 3);
    List<LocatedBlock> blocks1 = DFSTestUtil.getAllBlocks(fs, path);
    assertTrue(blocks1.size() > 0);
    assertEquals(distribution, ZoneMover.getBlockDistribution(blocks1.get(0)));

    // start datanodes in dc1
    final String[] hosts2 = {"host3", "host4", "host5"};
    final String[] racks2 = {"/dc1/rack0", "/dc1/rack1", "/dc1/rack2"};
    cluster.startDataNodes(conf, hosts2.length, true, null, racks2, hosts2, null, false);
    assertEquals(hosts1.length + hosts2.length, cluster.getDataNodes().size());

    // do block move
    Tool tool = new ZoneMover.Cli();
    tool.setConf(conf);
    final String[] args = {"-path", "/test", "-rule", "/dc1:3"};
    LOG.info("Try to do block move for path: /test ...");
    assertEquals(ExitStatus.SUCCESS.getExitCode(), tool.run(args));
    // sleep some time to wait datanode delete replicas
    Thread.sleep(DFS_HEARTBEAT_INTERVAL * 10 * 1000);

    // validate replica distribution after moving
    Map<String, Short> mapRule = new HashMap<>();
    mapRule.put("/dc1", (short) 3);
    List<LocatedBlock> blocks2 = DFSTestUtil.getAllBlocks(fs, path);
    assertTrue(blocks1.size() > 0);
    assertEquals(mapRule, ZoneMover.getBlockDistribution(blocks2.get(0)));
  }

  @After
  public void teardown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }
}
