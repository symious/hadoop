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
package org.apache.hadoop.tools.ec;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfoWithStorage;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.tools.DebugAdmin;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Random;

import static org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetTestUtil.getBlockFile;
import static org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetTestUtil.getMetaFile;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * test {@link ECValidatorJob}
 */
public class TestECValidatorJob {

  private Path inputPath;
  private Path outputPath;
  private MiniDFSCluster dfsCluster;
  private Configuration conf;
  private DistributedFileSystem fs;
  private DebugAdmin admin;

  private static final String inputDir = "input";
  private static final String outputDir = "output";
  private static final String ns1 = "ns1";
  private static final String ns2 = "ns2";
  private final ErasureCodingPolicy ecPolicy = SystemErasureCodingPolicies.getByID(
      SystemErasureCodingPolicies.XOR_2_1_POLICY_ID);

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.set(CapacitySchedulerConfiguration.PREFIX
        + CapacitySchedulerConfiguration.ROOT + "."
        + CapacitySchedulerConfiguration.QUEUES, "default");
    conf.set(CapacitySchedulerConfiguration.PREFIX
        + CapacitySchedulerConfiguration.ROOT + ".default."
        + CapacitySchedulerConfiguration.CAPACITY, "100");
    dfsCluster = new MiniDFSCluster.Builder(conf).checkExitOnShutdown(true)
        .numDataNodes(3).format(true).racks(null).build();
    fs = dfsCluster.getFileSystem();
    admin = new DebugAdmin(conf);
  }

  @After
  public void tearDown() throws Exception {
    if (dfsCluster != null) {
      dfsCluster.shutdown();
      dfsCluster = null;
    }
  }

  @Test(timeout = 600000)
  public void testECValidator() throws Exception {
    // create hdfs data.
    createData();

    // run ec validator job.
    ECValidatorJob ecValidatorJob = new ECValidatorJob(conf);
    final String[] args = { "-i", inputPath.toString(), "-o", outputPath.toString(), "-m", "3",
        "-r", "1"};
    int exitCode = 0;
    Assert.assertEquals(exitCode, ToolRunner.run(ecValidatorJob, args));

    assertTrue(fs.exists(outputPath));
    FileStatus[] status = fs.listStatus(outputPath);
    for (FileStatus file : status) {
      if (!file.getPath().getName().startsWith("part-")) {
        continue;
      }
      String content = DFSTestUtil.readFile(fs, file.getPath());
      String contents [] = content.split("\n");
      Assert.assertEquals(9, contents.length);
      Assert.assertTrue(contents[0].contains("no ns|/ec_dir/no_ns|failed|no valid"));
      Assert.assertTrue(contents[1].contains(ns1 + "|/bar|failed|File /bar is not erasure coded"));
      Assert.assertTrue(contents[2].contains(ns1 + "|/bar_not_exist|failed|File " +
          "/bar_not_exist does not exist"));
      Assert.assertTrue(contents[3].contains(ns2 + "|/ec_dir|failed|File /ec_dir is not " +
          "a regular file"));
      Assert.assertTrue(contents[4].contains(ns2 + "|/ec_dir/foo|failed|File /ec_dir/foo " +
          "is not closed"));
      Assert.assertTrue(contents[5].contains(ns2 + "|/ec_dir/foo_1m|healthy"));
      Assert.assertTrue(contents[6].contains(ns2 + "|/ec_dir/foo_6m|healthy"));
      Assert.assertTrue(contents[7].contains(ns2 + "|/ec_dir/foo_checksum_failed|failed"));
      Assert.assertTrue(contents[8].contains(ns2 + "|/ec_dir/foo_corrupt|failed"));
    }
  }

  private void createData() throws IOException {
    final short repl = 1;
    final long k = 1024;
    final long m = k * k;
    final long seed = 0x1234567L;

    // prepare input path.
    inputPath = new Path(fs.getHomeDirectory(), inputDir);
    fs.delete(inputPath, true);
    fs.mkdirs(inputPath);

    // prepare output path.
    outputPath = new Path(fs.getHomeDirectory(), outputDir);
    fs.delete(outputPath, true);

    // create rep file.
    fs.create(new Path("/bar")).close();

    // create ec dir.
    final Path ecDir = new Path("/ec_dir");
    fs.mkdir(ecDir, FsPermission.getDirDefault());
    fs.enableErasureCodingPolicy(ecPolicy.getName());
    fs.setErasureCodingPolicy(ecDir, ecPolicy.getName());

    // create ec file and not close.
    fs.create(new Path(ecDir, "foo"));

    // create normal ec file.
    DFSTestUtil.createFile(fs, new Path(ecDir, "foo_1m"), m, repl, seed);
    DFSTestUtil.createFile(fs, new Path(ecDir, "foo_6m"), (int) k, 6 * m, m, repl,
        seed);

    // create corrupt ec file.
    Path corruptFile = new Path(ecDir, "foo_corrupt");
    DFSTestUtil.createFile(fs, corruptFile, 5841961, repl, seed);
    List<LocatedBlock> blocks = DFSTestUtil.getAllBlocks(fs, corruptFile);
    assertEquals(1, blocks.size());
    LocatedStripedBlock blockGroup = (LocatedStripedBlock) blocks.get(0);
    LocatedBlock[] indexedBlocks = StripedBlockUtil.parseStripedBlockGroup(blockGroup,
        ecPolicy.getCellSize(), ecPolicy.getNumDataUnits(), ecPolicy.getNumParityUnits());
    // Try corrupt block 0 in block group.
    LocatedBlock toCorruptLocatedBlock = indexedBlocks[0];
    ExtendedBlock toCorruptBlock = toCorruptLocatedBlock.getBlock();
    DataNode datanode = dfsCluster.getDataNode(toCorruptLocatedBlock.getLocations()[0].
        getIpcPort());
    File blockFile = getBlockFile(datanode.getFSDataset(),
        toCorruptBlock.getBlockPoolId(), toCorruptBlock.getLocalBlock());
    File metaFile = getMetaFile(datanode.getFSDataset(),
        toCorruptBlock.getBlockPoolId(), toCorruptBlock.getLocalBlock());
    // Write error bytes to block file and re-generate meta checksum.
    byte[] errorBytes = new byte[2097152];
    new Random(seed).nextBytes(errorBytes);
    FileUtils.writeByteArrayToFile(blockFile, errorBytes);
    metaFile.delete();
    runCmd(new String[]{"computeMeta", "-block", blockFile.getAbsolutePath(),
        "-out", metaFile.getAbsolutePath()});

    // create checkSumFailed ec file.
    Path checkSumFailedFile = new Path(ecDir, "foo_checksum_failed");
    DFSTestUtil.createFile(fs, checkSumFailedFile, 5841961, repl, seed);
    blocks = DFSTestUtil.getAllBlocks(fs, checkSumFailedFile);
    assertEquals(1, blocks.size());
    blockGroup = (LocatedStripedBlock) blocks.get(0);
    indexedBlocks = StripedBlockUtil.parseStripedBlockGroup(blockGroup,
        ecPolicy.getCellSize(), ecPolicy.getNumDataUnits(), ecPolicy.getNumParityUnits());
    // Try checkSumFailed block 0 in block group.
    LocatedBlock toFailedLocatedBlock = indexedBlocks[0];
    ExtendedBlock toFailedBlock = toFailedLocatedBlock.getBlock();
    DatanodeInfoWithStorage datanodeInfoWithStorage = toFailedLocatedBlock.getLocations()[0];
    datanode = dfsCluster.getDataNode(datanodeInfoWithStorage.getIpcPort());
    blockFile = getBlockFile(datanode.getFSDataset(),
        toFailedBlock.getBlockPoolId(), toFailedBlock.getLocalBlock());

    // Write error bytes to block file and not to update meta file, trigger ChecksumException.
    errorBytes = new byte[2097152];
    new Random(seed).nextBytes(errorBytes);
    FileUtils.writeByteArrayToFile(blockFile, errorBytes);

    // write file into input dir.
    String file1 = "file1";
    String file2 = "file2";
    StringBuilder stringBuilder1 = new StringBuilder();
    stringBuilder1.append("/ec_dir/no_ns").append("\n").
        append(ns1).append("\t").append("/bar_not_exist").append("\n").
        append(ns1).append("\t").append("/bar").append("\n").
        append(ns2).append("\t").append("/ec_dir").append("\n").
        append(ns2).append("\t").append("/ec_dir/foo");
    DFSTestUtil.createFile(inputPath, file1, fs, stringBuilder1.toString().
        getBytes("UTF-8"));

    StringBuilder stringBuilder2 = new StringBuilder();
    stringBuilder2.append(ns2).append("\t").append("/ec_dir/foo_1m").append("\n").
        append(ns2).append("\t").append("/ec_dir/foo_6m").append("\n").
        append(ns2).append("\t").append("/ec_dir/foo_corrupt").append("\n").
        append(ns2).append("\t").append("/ec_dir/foo_checksum_failed");
    DFSTestUtil.createFile(inputPath, file2, fs, stringBuilder2.toString().
        getBytes("UTF-8"));
  }

  private String runCmd(String[] cmd) {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    final PrintStream out = new PrintStream(bytes);
    final PrintStream oldErr = System.err;
    final PrintStream oldOut = System.out;
    System.setErr(out);
    System.setOut(out);
    int ret;
    try {
      ret = admin.run(cmd);
    } finally {
      System.setErr(oldErr);
      System.setOut(oldOut);
      IOUtils.closeStream(out);
    }
    return "ret: " + ret + ", " +
        bytes.toString().replaceAll(System.lineSeparator(), "");
  }
}
