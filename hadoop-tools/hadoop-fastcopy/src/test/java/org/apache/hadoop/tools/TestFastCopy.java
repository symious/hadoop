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
package org.apache.hadoop.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeInfoWithStorage;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.tools.FastCopy.FastFileCopyRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestFastCopy {
  private Configuration conf = null;
  private MiniDFSCluster cluster = null;
  private DistributedFileSystem srcDFS = null;
  private DistributedFileSystem dstDFS = null;

  private final Path file0 = new Path("/testFastCopy/NoBlocks");
  private final Path file1 = new Path("/testFastCopy/TenBlocks");
  private final Path file2 = new Path("/testFastCopy/sub/NoBlocks");
  private final Path file3 = new Path("/testFastCopy/sub/TenBlocks");
  private final Path ecDir = new Path("/testFastCopyWithEC");
  private final Path subEcDir = new Path(ecDir, "sub");
  private final Path file_ec_0 = new Path(ecDir,"NoBlocks");
  private final Path file_ec_1 = new Path(ecDir,"OneBlocks");
  private final Path file_ec_2 = new Path(ecDir,"TwoBlocks");
  private final Path file_ec_3 = new Path(subEcDir, "NoBlocks");
  private final Path file_ec_4 = new Path(subEcDir, "OneBlocks");
  private final Path file_ec_5 = new Path(subEcDir, "TwoBlocks");

  private final ErasureCodingPolicy ecPolicy = SystemErasureCodingPolicies.getByID(
      SystemErasureCodingPolicies.XOR_2_1_POLICY_ID);
  private final short dataBlocks = (short) ecPolicy.getNumDataUnits();

  @Before
  public void setup() throws Exception {
    try {
      conf = new HdfsConfiguration();
      long blockSize = 1024 * 1024;
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
      conf.setBoolean(CommonConfigurationKeys.IGNORE_SDI_AUTHENTICATE_KEY,
          true);

      cluster = new MiniDFSCluster.Builder(conf)
          .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(2))
          .numDataNodes(6).build();

      cluster.waitActive();

      srcDFS = cluster.getFileSystem(0);
      dstDFS = cluster.getFileSystem(1);

      short replication = 2;
      DFSTestUtil.createFile(srcDFS, file0, 0, replication, 0L);
      DFSTestUtil.createFile(srcDFS, file1, 10 * blockSize, replication, 0L);
      DFSTestUtil.createFile(srcDFS, file2, 0, replication, 0L);
      DFSTestUtil.createFile(srcDFS, file3, 10 * blockSize, replication, 0L);

      // Create ec dir and file.
      srcDFS.mkdir(ecDir, FsPermission.getDirDefault());
      srcDFS.enableErasureCodingPolicy(ecPolicy.getName());
      srcDFS.setErasureCodingPolicy(ecDir, ecPolicy.getName());
      srcDFS.mkdirs(subEcDir);

      DFSTestUtil.createFile(srcDFS, file_ec_0, 0, (short) 1, 0L);

      byte[] expected = StripedFileTestUtil.generateBytes((int) (blockSize * dataBlocks));
      DFSTestUtil.writeFile(srcDFS, file_ec_1, new String(expected));
      StripedFileTestUtil.waitBlockGroupsReported(srcDFS, file_ec_1.toString());

      expected = StripedFileTestUtil.generateBytes((int) (blockSize * dataBlocks * 2));
      DFSTestUtil.writeFile(srcDFS, file_ec_2, new String(expected));
      StripedFileTestUtil.waitBlockGroupsReported(srcDFS, file_ec_2.toString());

      DFSTestUtil.createFile(srcDFS, file_ec_3, 0, (short) 1, 0L);

      expected = StripedFileTestUtil.generateBytes((int) (blockSize * dataBlocks));
      DFSTestUtil.writeFile(srcDFS, file_ec_4, new String(expected));
      StripedFileTestUtil.waitBlockGroupsReported(srcDFS, file_ec_4.toString());

      expected = StripedFileTestUtil.generateBytes((int) (blockSize * dataBlocks * 2));
      DFSTestUtil.writeFile(srcDFS, file_ec_5, new String(expected));
      StripedFileTestUtil.waitBlockGroupsReported(srcDFS, file_ec_5.toString());
    } catch (Exception e) {
      cluster.shutdown();
      throw e;
    }
  }

  @After
  public void shutdown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testFastCopy() throws Exception {

    FastCopy fcp = new FastCopy(conf, 2, true);

    List<FastFileCopyRequest> requests = new ArrayList<>();

    requests.add(new FastFileCopyRequest(file0.toString(),
        file0.toString(), srcDFS, dstDFS));
    requests.add(new FastFileCopyRequest(file1.toString(),
        file1.toString(), srcDFS, dstDFS));
    requests.add(new FastFileCopyRequest(file2.toString(),
        file2.toString(), srcDFS, dstDFS));
    requests.add(new FastFileCopyRequest(file3.toString(),
        file3.toString(), srcDFS, dstDFS));
    fcp.copy(requests);
    fcp.shutdown();

    assertTrue(dstDFS.getFileStatus(file0).isFile());
    assertTrue(dstDFS.getFileStatus(file1).isFile());
    assertTrue(dstDFS.getFileStatus(file2).isFile());
    assertTrue(dstDFS.getFileStatus(file3).isFile());

    assertEquals(dstDFS.getFileChecksum(file0), srcDFS.getFileChecksum(file0));
    assertEquals(dstDFS.getFileChecksum(file1), srcDFS.getFileChecksum(file1));
    assertEquals(dstDFS.getFileChecksum(file2), srcDFS.getFileChecksum(file2));
    assertEquals(dstDFS.getFileChecksum(file3), srcDFS.getFileChecksum(file3));
  }

  @Test
  public void testFastCopyWithECFile() throws Exception {

    FastCopy fcp = new FastCopy(conf, 2, true);
    String[] fileNames = {
        file_ec_0.toString(),
        file_ec_1.toString(),
        file_ec_2.toString(),
        file_ec_3.toString(),
        file_ec_4.toString(),
        file_ec_5.toString()
    };

    List<FastFileCopyRequest> requests = new ArrayList<>();
    for (String fileName : fileNames) {
      requests.add(new FastFileCopyRequest(fileName, fileName, srcDFS, dstDFS));
    }
    fcp.copy(requests);
    fcp.shutdown();

    assertFilesAreErasureCoded(srcDFS, fileNames);
    assertFilesAreErasureCoded(dstDFS, fileNames);
    assertChecksumsAndLengthAreEqual(srcDFS, dstDFS, fileNames);
    assertDatanodeUuidsEqual(srcDFS, dstDFS, fileNames);
  }

  private void assertFilesAreErasureCoded(DistributedFileSystem fs, String[] fileNames)
      throws IOException {
    for (String fileName : fileNames) {
      assertTrue(fs.getFileStatus(new Path(fileName)).isFile());
      assertTrue(fs.getFileStatus(new Path(fileName)).isErasureCoded());
    }
  }

  private void assertChecksumsAndLengthAreEqual(DistributedFileSystem srcFS,
      DistributedFileSystem dstFS, String[] fileNames) throws IOException {
    for (String fileName : fileNames) {
      assertEquals(srcFS.getFileChecksum(new Path(fileName)),
          dstFS.getFileChecksum(new Path(fileName)));
      assertEquals(srcFS.getFileStatus(new Path(fileName)).getLen(),
          dstFS.getFileStatus(new Path(fileName)).getLen());
    }
  }

  private void assertDatanodeUuidsEqual(DistributedFileSystem srcFS,
      DistributedFileSystem dstFS, String[] fileNames) throws IOException {
    for (String fileName : fileNames) {
      List<String> listSrc = getDatanodeUuids(new Path(fileName), srcFS);
      List<String> listDst = getDatanodeUuids(new Path(fileName), dstFS);
      assertEquals(listSrc, listDst);
    }
  }

  private List<String> getDatanodeUuids(Path filePath, DistributedFileSystem fs)
      throws IOException {
    LocatedBlocks locatedBlocks = StripedFileTestUtil.getLocatedBlocks(filePath, fs);
    return locatedBlocks.getLocatedBlocks().stream()
        .flatMap(block -> Arrays.stream(block.getLocations()))
        .map(DatanodeInfoWithStorage::getDatanodeUuid)
        .collect(Collectors.toList());
  }
}
