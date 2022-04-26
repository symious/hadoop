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
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.tools.FastCopy.FastFileCopyRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

  @Before
  public void setup() throws IOException {
    try {
      conf = new HdfsConfiguration();
      long blockSize = 1024 * 1024;
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
      conf.setBoolean(CommonConfigurationKeys.IGNORE_SDI_AUTHENTICATE_KEY,
          true);

      cluster = new MiniDFSCluster.Builder(conf)
          .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(2))
          .numDataNodes(3).build();

      cluster.waitActive();

      srcDFS = cluster.getFileSystem(0);
      dstDFS = cluster.getFileSystem(1);

      short replication = 2;
      DFSTestUtil.createFile(srcDFS, file0, 0, replication, 0L);
      DFSTestUtil.createFile(srcDFS, file1, 10 * blockSize, replication, 0L);
      DFSTestUtil.createFile(srcDFS, file2, 0, replication, 0L);
      DFSTestUtil.createFile(srcDFS, file3, 10 * blockSize, replication, 0L);
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
}
