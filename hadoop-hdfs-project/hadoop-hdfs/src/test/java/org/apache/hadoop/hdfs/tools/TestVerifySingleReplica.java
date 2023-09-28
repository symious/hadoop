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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY;

public class TestVerifySingleReplica {

  private static MiniDFSCluster cluster;
  private static Configuration conf;

  private static final long BLOCK_SIZE = 1024;
  private static final int FILE_SIZE_IN_BLOCKS = 10;
  private static final byte[] DUMMY_DATA = new byte[(int) BLOCK_SIZE * FILE_SIZE_IN_BLOCKS];

  private static DistributedFileSystem fs;

  @Before
  public void setupCluster() throws Exception {
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
    conf.setLong(DFSConfigKeys.FS_TRASH_INTERVAL_KEY, 1);
    // Stop early to save time
    conf.setInt(DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY, 1);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();

    fs = cluster.getFileSystem();
  }

  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testDeadDN() throws Exception {
    Path testPath = new Path("/testFileWithMissingBlocks1");
    DFSTestUtil.writeFile(fs, testPath, DUMMY_DATA);
    purgeBlockByKillingDN(testPath);
    Thread.sleep(1000);
    internalTestFileWithMissingBlocks(testPath);
  }

  @Test
  public void testDeletedBlock() throws Exception {
    Path testPath = new Path("/testFileWithMissingBlocks2");
    DFSTestUtil.writeFile(fs, testPath, DUMMY_DATA);
    purgeBlockByDeleting(testPath);
    internalTestFileWithMissingBlocks(testPath);
  }

  private void internalTestFileWithMissingBlocks(Path testPath) throws Exception {
    // Then a path that is completely normal
    Path normalPath = new Path("/testFileWithMissingBlocksPresent");
    DFSTestUtil.writeFile(fs, normalPath, DUMMY_DATA);

    File inputFile = File.createTempFile("testFileWithMissingBlocks-input", null);
    File previewFile = File.createTempFile("testFileWithMissingBlocks-preview", null);
    inputFile.deleteOnExit();
    previewFile.deleteOnExit();
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(inputFile))) {
      bw.write(testPath.toString());
      bw.write("\n");
      bw.write(normalPath.toString());
      bw.write("\n");
      bw.flush();
    }

    ToolRunner.run(conf, new VerifySingleReplica(),
        new String[] { "-i", inputFile.getAbsolutePath(), "-p", previewFile.getAbsolutePath() });

    Pair<Set<String>, Set<String>> result = parsePreviewFile(previewFile.getAbsolutePath());
    Set<String> filesWithMissingBlocks = new HashSet<>();
    filesWithMissingBlocks.add(testPath.toString());
    Set<String> filesToAddReplica = new HashSet<>();
    filesToAddReplica.add(normalPath.toString());
    Assert.assertEquals(result.getLeft(), filesWithMissingBlocks);
    Assert.assertEquals(result.getRight(), filesToAddReplica);

    ToolRunner.run(conf, new VerifySingleReplica(),
        new String[] { "-i", inputFile.getAbsolutePath(), "-p", previewFile.getAbsolutePath(), "-e" });

    Assert.assertFalse(fs.exists(testPath));
    Assert.assertEquals(2, fs.getFileStatus(normalPath).getReplication());
  }

  private Pair<Set<String>, Set<String>> parsePreviewFile(String previewFile) {
    Set<String> pathsToDelete = new HashSet<>();
    Set<String> pathsToSetReplica = new HashSet<>();

    try (BufferedReader br = new BufferedReader(new FileReader(previewFile))) {
      String line = br.readLine();
      while (line != null) {
        String[] lineSplit = line.trim().split(",");
        if (Objects.equals(lineSplit[1], "1")) {
          pathsToDelete.add(lineSplit[0]);
        } else {
          pathsToSetReplica.add(lineSplit[0]);
        }
        line = br.readLine();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return Pair.of(pathsToDelete, pathsToSetReplica);
  }

  private void purgeBlockByDeleting(Path testPath)
      throws IOException, InterruptedException, TimeoutException {
    HdfsLocatedFileStatus status = (HdfsLocatedFileStatus) fs.listFiles(testPath, true).next();
    int purgeIdx = new Random().nextInt(FILE_SIZE_IN_BLOCKS);
    LocatedBlock blockToPurge = status.getLocatedBlocks().get(purgeIdx);
    DataNodeTestUtils.getFSDataset(cluster.getDataNode(blockToPurge.getLocations()[0].getIpcPort()))
        .invalidate(cluster.getNamesystem().getBlockPoolId(),
            new Block[] { blockToPurge.getBlock().getLocalBlock() });
    GenericTestUtils.waitFor(() -> {
      try {
        return ((HdfsLocatedFileStatus) fs.listFiles(testPath, true)
            .next()).getLocatedBlocks().get(purgeIdx).getLocations().length == 0;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }, 100, 10000);
  }

  private void purgeBlockByKillingDN(Path testPath) throws IOException {
    HdfsLocatedFileStatus status = (HdfsLocatedFileStatus) fs.listFiles(testPath, true).next();
    int purgeIdx = new Random().nextInt(FILE_SIZE_IN_BLOCKS);
    LocatedBlock blockToPurge = status.getLocatedBlocks().get(purgeIdx);
    cluster.getDataNode(blockToPurge.getLocations()[0].getIpcPort()).shutdown();
  }
}
