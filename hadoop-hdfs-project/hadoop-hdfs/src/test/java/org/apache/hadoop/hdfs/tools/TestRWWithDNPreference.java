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
package org.apache.hadoop.hdfs.tools;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSClientFaultInjector;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfoWithStorage;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestRWWithDNPreference {

  GenericTestUtils.LogCapturer logs =
      GenericTestUtils.LogCapturer.captureLogs(DFSClient.LOG);
  private static Configuration conf = new Configuration();
  private static MiniDFSCluster cluster;
  private static DistributedFileSystem fs;
  private static final String TEST_FILE = "testRWWithDNPreference.txt";
  private static final String TEST_PATH = "/" + TEST_FILE;
  private static final int BLOCK_SIZE = 1 << 10;

  @BeforeClass
  public static void setup() throws IOException {
    LogManager.getLogger(DFSClient.class.getName()).setLevel(Level.DEBUG);
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
    new File(TEST_FILE).delete();
  }

  @Test
  public void testReadSingleBlock() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(5).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();

    DFSTestUtil.writeFile(fs, new Path(TEST_PATH), "test data".getBytes());

    DatanodeInfoWithStorage[] realDns =
        fs.getClient().getLocatedBlocks(TEST_PATH, 0).get(0).getLocations();
    // Read only from the 3rd DN
    List<InetSocketAddress> favored = Collections.singletonList(realDns[2].getResolvedAddress());
    List<InetSocketAddress> ignored = new ArrayList<>();
    ignored.add(realDns[0].getResolvedAddress());
    ignored.add(realDns[1].getResolvedAddress());

    logs.clearOutput();
    setupAndRun(new String[] { "debugRead", TEST_PATH }, favored, ignored);
    verifyRead("test data");
    // Check if the 1st DN is used
    Assert.assertTrue(
        logs.getOutput().contains("Connecting to datanode " + realDns[2].getXferAddr()));
  }

  @Test
  public void testReadMultipleBlocks() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 5);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(5).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    int TEST_BLOCK_COUNT = 64;
    String content = RandomStringUtils.randomAlphabetic(BLOCK_SIZE * TEST_BLOCK_COUNT);

    DFSTestUtil.writeFile(fs, new Path(TEST_PATH), content.getBytes());

    DatanodeInfoWithStorage[] realDns =
        fs.getClient().getLocatedBlocks(TEST_PATH, 0).get(0).getLocations();
    // Read only from the last DN
    List<InetSocketAddress> favored = Collections.singletonList(realDns[4].getResolvedAddress());
    List<InetSocketAddress> ignored = new ArrayList<>();
    ignored.add(realDns[0].getResolvedAddress());
    ignored.add(realDns[1].getResolvedAddress());
    ignored.add(realDns[2].getResolvedAddress());
    ignored.add(realDns[3].getResolvedAddress());

    logs.clearOutput();
    setupAndRun(new String[] { "debugRead", TEST_PATH }, favored, ignored);
    verifyRead(content);
    // Check if the last DN is used exactly 64 times for 64 blocks
    Assert.assertEquals(TEST_BLOCK_COUNT, StringUtils.countMatches(logs.getOutput(),
        "Connecting to datanode " + realDns[4].getXferAddr()));
  }

  @Test
  public void testReadMultipleBlocksFailed() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    // Fail in 1 attempt for faster testing
    conf.setInt(DFSConfigKeys.DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY, 1);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(5).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    int TEST_BLOCK_COUNT = 16;
    String content = RandomStringUtils.randomAlphabetic(BLOCK_SIZE * TEST_BLOCK_COUNT);

    DFSTestUtil.writeFile(fs, new Path(TEST_PATH), content.getBytes());

    DatanodeInfoWithStorage[] realDns =
        fs.getClient().getLocatedBlocks(TEST_PATH, 0).get(6).getLocations();
    // Ignore all DNs from one block
    List<InetSocketAddress> ignored = new ArrayList<>();
    ignored.add(realDns[0].getResolvedAddress());
    ignored.add(realDns[1].getResolvedAddress());
    ignored.add(realDns[2].getResolvedAddress());

    logs.clearOutput();
    setupAndRun(new String[] { "debugRead", TEST_PATH }, new ArrayList<>(), ignored);
    Assert.assertTrue(logs.getOutput().contains("BlockMissingException"));
  }


  @Test
  public void testWriteSingleBlock() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(5).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();

    writeTestFile("test data");

    // Prefer the first 3 DNs
    List<InetSocketAddress> favored = new ArrayList<>();
    List<InetSocketAddress> ignored = new ArrayList<>();
    favored.add(cluster.getDataNodes().get(0).getXferAddress());
    favored.add(cluster.getDataNodes().get(1).getXferAddress());
    favored.add(cluster.getDataNodes().get(2).getXferAddress());
    ignored.add(cluster.getDataNodes().get(3).getXferAddress());
    ignored.add(cluster.getDataNodes().get(4).getXferAddress());

    logs.clearOutput();
    setupAndRun(new String[] { "debugWrite", TEST_FILE, TEST_PATH }, favored, ignored);
    assertDns(favored);
    verifyRead("test data");
  }

  @Test
  public void testWriteMultipleBlocks() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(5).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    int TEST_BLOCK_COUNT = 64;
    String content = RandomStringUtils.randomAlphabetic(BLOCK_SIZE * TEST_BLOCK_COUNT);
    writeTestFile(content);

    // Prefer the last 3 DNs
    List<InetSocketAddress> favored = new ArrayList<>();
    List<InetSocketAddress> ignored = new ArrayList<>();
    ignored.add(cluster.getDataNodes().get(0).getXferAddress());
    ignored.add(cluster.getDataNodes().get(1).getXferAddress());
    favored.add(cluster.getDataNodes().get(2).getXferAddress());
    favored.add(cluster.getDataNodes().get(3).getXferAddress());
    favored.add(cluster.getDataNodes().get(4).getXferAddress());

    logs.clearOutput();
    setupAndRun(new String[] { "debugWrite", TEST_FILE, TEST_PATH }, favored, ignored);
    assertDns(favored);
    verifyRead(content);
  }

  @Test
  public void testWriteMultipleBlocksFailed() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(5).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    int TEST_BLOCK_COUNT = 16;
    String content = RandomStringUtils.randomAlphabetic(BLOCK_SIZE * TEST_BLOCK_COUNT);
    writeTestFile(content);

    // Prefer 1 dead DN
    List<InetSocketAddress> favored = new ArrayList<>();
    List<InetSocketAddress> ignored = new ArrayList<>();
    favored.add(cluster.getDataNodes().get(0).getXferAddress());
    favored.add(cluster.getDataNodes().get(1).getXferAddress());
    favored.add(cluster.getDataNodes().get(2).getXferAddress());
    ignored.add(cluster.getDataNodes().get(3).getXferAddress());
    ignored.add(cluster.getDataNodes().get(4).getXferAddress());
    cluster.shutdownDataNode(2);

    logs.clearOutput();
    setupAndRun(new String[] { "debugWrite", TEST_FILE, TEST_PATH }, favored, ignored);
    // Only written to the 1st 2 DNs
    assertDns(favored.subList(0,2));
    verifyRead(content);
  }

  @Test
  public void testReadNonStrict() throws Exception {
    // Write to all DNs
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 5);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(5).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();

    DFSTestUtil.writeFile(fs, new Path(TEST_PATH), "test data".getBytes());

    DatanodeInfoWithStorage[] realDns =
        fs.getClient().getLocatedBlocks(TEST_PATH, 0).get(0).getLocations();
    // Read only from the 1st DN
    List<InetSocketAddress> favored = Collections.singletonList(realDns[0].getResolvedAddress());

    logs.clearOutput();
    setupAndRun(new String[] { "debugRead", TEST_PATH }, favored, new ArrayList<>());
    verifyRead("test data");
    // Check if the 1st DN is used
    Assert.assertTrue(
        logs.getOutput().contains("Connecting to datanode " + realDns[0].getXferAddr()));
  }

  @Test
  public void testWriteNonStrict() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(5).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();

    writeTestFile("test data");

    // Prefer the first DN
    List<InetSocketAddress> favored = new ArrayList<>();
    List<InetSocketAddress> ignored = new ArrayList<>();
    favored.add(cluster.getDataNodes().get(0).getXferAddress());

    boolean testPassed = false;
    cluster.getFileSystem().delete(new Path(TEST_PATH));
    setupAndRun(new String[] { "debugWrite", TEST_FILE, TEST_PATH }, favored, ignored);
    // Check if the 1st DN is used for one of the replica.
    DatanodeInfoWithStorage[] dns =
        cluster.getFileSystem().getClient().getLocatedBlocks(TEST_PATH, 0).get(0).getLocations();
    for (DatanodeInfoWithStorage dn : dns) {
      testPassed |= dn.getXferAddr()
          .equals(cluster.getDataNodes().get(0).getXferAddress().toString().substring(1));
    }
    Assert.assertTrue(testPassed);
    verifyRead("test data");
  }

//  @Test
  public void testWriteNonStrictWithSlowNodes() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    conf.setBoolean(HdfsClientConfigKeys.DFS_CLIENT_TREAT_SLOWNODE_AS_BADNODE_KEY, true);
    conf.setBoolean(HdfsClientConfigKeys.DFS_CLIENT_AVOID_SLOW_DATANODES_FOR_READ_KEY, true);
    // Faster timeout for faster test
    conf.setLong(HdfsClientConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, 5000);
    conf.setInt(HdfsClientConfigKeys.DFS_CLIENT_TREAT_SLOWNODE_AS_BADNODE_THRESHOLD_KEY, 1);
    conf.setInt(HdfsClientConfigKeys.DFS_CLIENT_SLOW_NODE_CACHE_THRESHOLD_MS_KEY, 1500);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(4).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();

    int TEST_BLOCK_COUNT = 2;

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < BLOCK_SIZE * TEST_BLOCK_COUNT; i++) {
      sb.append("f");
    }
    String content = sb.toString();
    writeTestFile(content);

    AtomicBoolean throttled = new AtomicBoolean(false);

    DFSClientFaultInjector.set(new DFSClientFaultInjector() {
      @Override
      public void throttleConnectionBetweenDNs(DatanodeInfo[] nodes) {
        if (throttled.get()) {
          return;
        }
        Configuration throttledConf = new Configuration(conf);
        throttledConf.setLong(DFSConfigKeys.DFS_DATANODE_DATA_READ_BANDWIDTHPERSEC_KEY, 1);
        throttledConf.setLong(DFSConfigKeys.DFS_DATANODE_DATA_WRITE_BANDWIDTHPERSEC_KEY, 1);
        throttledConf.setLong(DFSConfigKeys.DFS_DATANODE_DATA_TRANSFER_BANDWIDTHPERSEC_KEY, 1);
        try {
          cluster.getDataNode(nodes[1].getIpcPort()).refreshThrottlerConfig(throttledConf);
          throttled.set(true);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });

    // Prefer the first 3 DNs
    List<InetSocketAddress> favored = new ArrayList<>();
    List<InetSocketAddress> ignored = new ArrayList<>();
    favored.add(cluster.getDataNodes().get(0).getXferAddress());
    favored.add(cluster.getDataNodes().get(1).getXferAddress());
    favored.add(cluster.getDataNodes().get(2).getXferAddress());

    cluster.getFileSystem().delete(new Path(TEST_PATH));
    setupAndRun(new String[] { "debugWrite", TEST_FILE, TEST_PATH }, favored, ignored);
    verifyRead(content);
  }


  @Test
  public void testHedgedReadNonStrictWithSlowNodes() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    conf.setBoolean(HdfsClientConfigKeys.DFS_CLIENT_TREAT_SLOWNODE_AS_BADNODE_KEY, true);
    conf.setBoolean(HdfsClientConfigKeys.DFS_CLIENT_AVOID_SLOW_DATANODES_FOR_READ_KEY, true);
    conf.setInt(HdfsClientConfigKeys.HedgedRead.THREADPOOL_SIZE_KEY, 3);
    conf.setLong(HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_KEY, 1000);
    // Faster timeout for faster test
    conf.setLong(HdfsClientConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, 3000);
    conf.setInt(HdfsClientConfigKeys.DFS_CLIENT_TREAT_SLOWNODE_AS_BADNODE_THRESHOLD_KEY, 1);
    conf.setInt(HdfsClientConfigKeys.DFS_CLIENT_SLOW_NODE_CACHE_THRESHOLD_MS_KEY, 1000);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();

    int TEST_BLOCK_COUNT = 64;
    String content = RandomStringUtils.randomAlphabetic(BLOCK_SIZE * TEST_BLOCK_COUNT);
    DFSTestUtil.writeFile(fs, new Path(TEST_PATH), content.getBytes());

    Configuration throttledConf = new Configuration(conf);
    throttledConf.setLong(DFSConfigKeys.DFS_DATANODE_DATA_READ_BANDWIDTHPERSEC_KEY, 1);
    throttledConf.setLong(DFSConfigKeys.DFS_DATANODE_DATA_WRITE_BANDWIDTHPERSEC_KEY, 1);
    throttledConf.setLong(DFSConfigKeys.DFS_DATANODE_DATA_TRANSFER_BANDWIDTHPERSEC_KEY, 1);
    cluster.getDataNodes().get(0).refreshThrottlerConfig(throttledConf);
    cluster.getDataNodes().get(1).refreshThrottlerConfig(throttledConf);

    // Prefer the first DN
    List<InetSocketAddress> favored = new ArrayList<>();
    List<InetSocketAddress> ignored = new ArrayList<>();
    favored.add(cluster.getDataNodes().get(0).getXferAddress());

    setupAndRun(new String[] { "debugRead", TEST_PATH }, favored, ignored);
    verifyRead(content);
  }

  private void assertDns(List<InetSocketAddress> testDNs)
      throws IOException, InterruptedException, TimeoutException {
    // Use a new client to prevent debug options from messing with checks
    DistributedFileSystem testFs = cluster.getFileSystem();
    for (LocatedBlock locatedBlock : testFs.getClient().getLocatedBlocks(TEST_PATH, 0)
        .getLocatedBlocks()) {
      GenericTestUtils.waitFor(() -> {
        return locatedBlock.getLocations().length == testDNs.size();
      }, 100, 3000);
      DatanodeInfoWithStorage[] realDNs = locatedBlock.getLocations();
      Set<String> set1 =
          testDNs.stream().map(x -> x.toString().substring(1)).collect(Collectors.toSet());
      Set<String> set2 = Arrays.stream(realDNs).map(DatanodeInfoWithStorage::getXferAddr)
          .collect(Collectors.toSet());
      Assert.assertEquals(set1, set2);
    }

  }

  private void writeTestFile(String content) throws IOException {
    BufferedWriter writer = new BufferedWriter(new FileWriter(TEST_FILE));
    writer.write(content);
    writer.close();
  }

  private void setupAndRun(String[] baseArgs, List<InetSocketAddress> favored,
      List<InetSocketAddress> ignored) throws Exception {
    String favoredStr =
        favored.stream().map(x -> x.toString().substring(1)).collect(Collectors.joining(","));
    String ignoredStr =
        ignored.stream().map(x -> x.toString().substring(1)).collect(Collectors.joining(","));
    List<String> args = new ArrayList<>(Arrays.asList(baseArgs));
    if (!favoredStr.isEmpty()) {
      args.add("-favored");
      args.add(favoredStr);
    }
    if (!ignoredStr.isEmpty()) {
      args.add("-excluded");
      args.add(ignoredStr);
    }
    ToolRunner.run(conf, new DebugAdmin(conf), args.toArray(new String[0]));
  }

  private void verifyRead(String content) throws IOException {
    InputStream reader = Files.newInputStream(new File(TEST_FILE).toPath());
    byte[] buff = new byte[4096];
    byte[] contentBytes = content.getBytes();
    int counter = 0;
    while (true) {
      int read = reader.read(buff);
      if (read == -1) {
        reader.close();
        return;
      }
      if (read < 4096) {
        Assert.assertArrayEquals(Arrays.copyOfRange(buff, 0, read),
            Arrays.copyOfRange(contentBytes, counter, counter + read));
      } else {
        Assert.assertArrayEquals(buff, Arrays.copyOfRange(contentBytes, counter, counter + read));
      }
      counter += read;
    }
  }
}
