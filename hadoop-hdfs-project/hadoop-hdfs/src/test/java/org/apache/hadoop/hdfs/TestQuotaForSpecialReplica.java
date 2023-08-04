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
package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.QuotaCounts;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Random;

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.OVERWRITE;
import static org.junit.Assert.assertEquals;

/** A class to test some quota-related RPCs for the special replication. */
public class TestQuotaForSpecialReplica {
  private static Configuration conf = null;
  private static MiniDFSCluster cluster;
  private static DistributedFileSystem dfs;
  private static final int DEFAULT_BLOCK_SIZE = 512;

  @BeforeClass
  public static void setUpClass() throws Exception {
    conf = new HdfsConfiguration();
    conf.set(
        MiniDFSCluster.HDFS_MINIDFS_BASEDIR,
        GenericTestUtils.getTestDir("my-test-quota-for-special").getAbsolutePath());
    conf.setInt("dfs.content-summary.limit", 4);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_CONTENT_SUMMARY_LIMIT_KEY, 2);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_RECOMPUTE_QUOTA_USAGE_ENABLE_KEY, true);
    restartCluster();

    dfs = cluster.getFileSystem();
  }

  private static void restartCluster() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(10).build();
    cluster.waitActive();
  }

  @AfterClass
  public static void tearDownClass() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testCreateRPC() throws IOException {
    Path path1 = new Path("/testCreateRPC");
    Path path2 = new Path("/testCreateRPC/testFile");
    Assert.assertTrue(dfs.mkdirs(path1));

    long fileSize = DEFAULT_BLOCK_SIZE + 10;
    short replication = 4;
    long spaceUsage = (replication - 1) * DEFAULT_BLOCK_SIZE * 2;
    dfs.setQuota(path1, Long.MAX_VALUE, spaceUsage + 10);

    // now creating childFile1 should succeed
    DFSTestUtil.createFile(dfs, path2, fileSize, replication, 0);

    QuotaUsage quotaUsage = dfs.getQuotaUsage(path1);
    spaceUsage = fileSize * (replication - 1);
    Assert.assertEquals(spaceUsage, quotaUsage.getSpaceConsumed());
    Assert.assertTrue(dfs.delete(path1, true));
  }

  @Test
  public void testWritingFile() throws IOException {
    Path path1 = new Path("/testWritingFile");
    Path path2 = new Path("/testWritingFile/testFile");
    Assert.assertTrue(dfs.mkdirs(path1));

    short replication = 4;
    long spaceUsage = DEFAULT_BLOCK_SIZE * 3;
    dfs.setQuota(path1, Long.MAX_VALUE, spaceUsage + 10);

    EnumSet<CreateFlag> createFlags = EnumSet.of(CREATE);
    createFlags.add(OVERWRITE);
    try (FSDataOutputStream out = dfs.create(path2,
        FsPermission.getFileDefault(), createFlags,
        4096, replication, DEFAULT_BLOCK_SIZE, null)) {

      // Test addBlock RPC
      int fileSize = DEFAULT_BLOCK_SIZE - 100;
      byte[] toWrite = new byte[fileSize];
      Random rb = new Random(1000);
      rb.nextBytes(toWrite);
      out.write(toWrite, 0, toWrite.length);
      out.hflush();

      QuotaUsage quotaUsage = dfs.getQuotaUsage(path1);
      // Normally the spaceUsage should be DEFAULT_BLOCK_SIZE * replication.
      Assert.assertEquals(spaceUsage, quotaUsage.getSpaceConsumed());
      Assert.assertEquals(spaceUsage, dfs.getContentSummary(path1).getSpaceConsumed());

      // Test complete RPC
      out.close();
      spaceUsage = fileSize * (replication - 1);
      quotaUsage = dfs.getQuotaUsage(path1);
      // Normally the spaceUsage should be fileSize * replication.
      Assert.assertEquals(spaceUsage, quotaUsage.getSpaceConsumed());
    } finally {
      Assert.assertTrue(dfs.delete(path1, true));
    }
  }

  @Test
  public void testGetContentSummary() throws IOException {
    Path path = new Path("/testGetContentSummary");
    Assert.assertTrue(dfs.mkdirs(path));

    long expectedSpaceUsage = 0;
    Path path1 = new Path("/testGetContentSummary/testFile1");
    long fileSize1 = DEFAULT_BLOCK_SIZE * 10 + 100;
    DFSTestUtil.createFile(dfs, path1, fileSize1, (short) 5, 0);
    expectedSpaceUsage += (fileSize1 * 3);

    Path path2 = new Path("/testGetContentSummary/testFile2");
    long fileSize2 = DEFAULT_BLOCK_SIZE * 10 - 200;
    DFSTestUtil.createFile(dfs, path2, fileSize2, (short) 3, 0);
    expectedSpaceUsage += (fileSize2 * 3);

    Path path3 = new Path("/testGetContentSummary/testFile3");
    long fileSize3 = DEFAULT_BLOCK_SIZE * 10 - 300;
    DFSTestUtil.createFile(dfs, path3, fileSize3, (short) 4, 0);
    expectedSpaceUsage += (fileSize3 * 3);

    ContentSummary contentSummary = dfs.getContentSummary(path);
    Assert.assertEquals(expectedSpaceUsage, contentSummary.getSpaceConsumed());
  }

  @Test
  public void testOverWrite() throws IOException {
    Path path = new Path("/testOverWrite");
    Assert.assertTrue(dfs.mkdirs(path));

    long expectedSpaceUsage = 0;
    Path path1 = new Path("/testOverWrite/testFile1");
    long fileSize1 = DEFAULT_BLOCK_SIZE * 10 + 100;
    DFSTestUtil.createFile(dfs, path1, fileSize1, (short) 5, 0);
    expectedSpaceUsage += (fileSize1 * 3);
    ContentSummary contentSummary = dfs.getContentSummary(path);
    Assert.assertEquals(expectedSpaceUsage, contentSummary.getSpaceConsumed());

    // Test Overwrite.
    long fileSize2 = DEFAULT_BLOCK_SIZE * 10 - 5;
    DFSTestUtil.createFile(dfs, path1, fileSize2, (short) 4, 0);
    expectedSpaceUsage -= (fileSize1 * 3);
    expectedSpaceUsage += (fileSize2 * 3);
    contentSummary = dfs.getContentSummary(path);
    Assert.assertEquals(expectedSpaceUsage, contentSummary.getSpaceConsumed());
  }

  @Test
  public void testRenameFile() throws IOException {
    Path pathSource = new Path("/testRenameFileSource");
    Assert.assertTrue(dfs.mkdirs(pathSource));
    dfs.setQuota(pathSource, Long.MAX_VALUE, Long.MAX_VALUE);
    Path pathTarget = new Path("/testRenameFileTarget");
    Assert.assertTrue(dfs.mkdirs(pathTarget));
    dfs.setQuota(pathTarget, Long.MAX_VALUE, Long.MAX_VALUE);

    Path path1 = new Path("/testRenameFileSource/testFile1");
    long fileSize1 = DEFAULT_BLOCK_SIZE * 10 + 100;
    DFSTestUtil.createFile(dfs, path1, fileSize1, (short) 5, 0);

    ContentSummary contentSummary = dfs.getContentSummary(pathSource);
    Assert.assertEquals(fileSize1 * 3, contentSummary.getSpaceConsumed());
    Assert.assertEquals(fileSize1 * 3, dfs.getQuotaUsage(pathSource).getSpaceConsumed());

    contentSummary = dfs.getContentSummary(pathTarget);
    Assert.assertEquals(0, contentSummary.getSpaceConsumed());
    Assert.assertEquals(0, dfs.getQuotaUsage(pathTarget).getSpaceConsumed());

    Path path2 = new Path("/testRenameFileTarget/testFile1");
    Assert.assertTrue(dfs.rename(path1, path2));
    contentSummary = dfs.getContentSummary(pathSource);
    Assert.assertEquals(0, contentSummary.getSpaceConsumed());
    Assert.assertEquals(0, dfs.getQuotaUsage(pathSource).getSpaceConsumed());

    contentSummary = dfs.getContentSummary(pathTarget);
    Assert.assertEquals(fileSize1 * 3, contentSummary.getSpaceConsumed());
    Assert.assertEquals(fileSize1 * 3, dfs.getQuotaUsage(pathTarget).getSpaceConsumed());

    Path path3 = new Path("/testRenameFileSource/testFile2");
    long fileSize3 = DEFAULT_BLOCK_SIZE * 10 + 200;
    DFSTestUtil.createFile(dfs, path3, fileSize3, (short) 6, 0);

    contentSummary = dfs.getContentSummary(pathSource);
    Assert.assertEquals(fileSize3 * 6, contentSummary.getSpaceConsumed());
    Assert.assertEquals(fileSize3 * 6, dfs.getQuotaUsage(pathSource).getSpaceConsumed());

    contentSummary = dfs.getContentSummary(pathTarget);
    Assert.assertEquals(fileSize1 * 3, contentSummary.getSpaceConsumed());
    Assert.assertEquals(fileSize1 * 3, dfs.getQuotaUsage(pathTarget).getSpaceConsumed());

    Path path4 = new Path("/testRenameFileTarget/testFile2");
    Assert.assertTrue(dfs.rename(path3, path4));
    contentSummary = dfs.getContentSummary(pathSource);
    Assert.assertEquals(0, contentSummary.getSpaceConsumed());
    Assert.assertEquals(0, dfs.getQuotaUsage(pathSource).getSpaceConsumed());

    contentSummary = dfs.getContentSummary(pathTarget);
    Assert.assertEquals(fileSize1 * 3 + fileSize3 * 6, contentSummary.getSpaceConsumed());
    Assert.assertEquals(fileSize1 * 3 + fileSize3 * 6,
        dfs.getQuotaUsage(pathTarget).getSpaceConsumed());

    Assert.assertTrue(dfs.delete(pathSource, true));
    Assert.assertTrue(dfs.delete(pathTarget, true));
  }

  @Test
  public void testDelete() throws IOException {
    Path path = new Path("/testDelete");
    Assert.assertTrue(dfs.mkdirs(path));
    dfs.setQuota(path, Long.MAX_VALUE, Long.MAX_VALUE);

    Path path1 = new Path("/testDelete/testFile1");
    long fileSize1 = DEFAULT_BLOCK_SIZE * 10 + 100;
    DFSTestUtil.createFile(dfs, path1, fileSize1, (short) 5, 0);

    ContentSummary contentSummary = dfs.getContentSummary(path);
    Assert.assertEquals(fileSize1 * 3, contentSummary.getSpaceConsumed());
    Assert.assertEquals(fileSize1 * 3, dfs.getQuotaUsage(path).getSpaceConsumed());

    Assert.assertTrue(dfs.delete(path1, true));

    contentSummary = dfs.getContentSummary(path);
    Assert.assertEquals(0, contentSummary.getSpaceConsumed());
    Assert.assertEquals(0, dfs.getQuotaUsage(path).getSpaceConsumed());
  }

  @Test
  public void testQuotaInitialization() throws Exception {
    final int fileNumber = 200;
    Path testDir = new Path("/testQuotaInitialization");
    long expectedSize = 3 * DEFAULT_BLOCK_SIZE + DEFAULT_BLOCK_SIZE/2;
    dfs.mkdirs(testDir);
    dfs.setQuota(testDir, fileNumber*4,
        DEFAULT_BLOCK_SIZE * 4 * fileNumber * 3 + 100);

    Path[] testDirs = new Path[fileNumber];
    for (int i = 0; i < fileNumber; i++) {
      testDirs[i] = new Path(testDir, "sub" + i);
      dfs.mkdirs(testDirs[i]);
      dfs.setQuota(testDirs[i], 100, 1000000);
      DFSTestUtil.createFile(dfs, new Path(testDirs[i], "a"), expectedSize,
          (short)5, 1L);
    }

    // Directly access the name system to obtain the current cached usage.
    INodeDirectory root = cluster.getNamesystem(0).getFSDirectory().getRoot();
    HashMap<String, Long> nsMap = new HashMap<String, Long>();
    HashMap<String, Long> dsMap = new HashMap<String, Long>();
    scanDirsWithQuota(root, nsMap, dsMap, false);

    updateCountForQuota(1);
    scanDirsWithQuota(root, nsMap, dsMap, true);

    updateCountForQuota(2);
    scanDirsWithQuota(root, nsMap, dsMap, true);

    updateCountForQuota(4);
    scanDirsWithQuota(root, nsMap, dsMap, true);

    Assert.assertEquals(expectedSize * 3 * fileNumber,
        dfs.getQuotaUsage(testDir).getSpaceConsumed());
  }

  @Test
  public void testSetReplication() throws Exception {

    // Create dir and set quota.
    Path dir = new Path("/testSetReplication");
    dfs.mkdirs(dir);
    dfs.setQuota(dir, 100, DEFAULT_BLOCK_SIZE * 5 + 100);

    // Create 3 replication file.
    Path file = new Path("/testSetReplication/file");
    short replication_3 = 3;
    long spaceUsage = replication_3 * DEFAULT_BLOCK_SIZE ;
    DFSTestUtil.createFile(dfs, file, DEFAULT_BLOCK_SIZE, replication_3, 0);
    // Validate dir spaceConsumed.
    QuotaUsage quotaUsage = dfs.getQuotaUsage(dir);
    Assert.assertEquals(spaceUsage, quotaUsage.getSpaceConsumed());
    ContentSummary contentSummary = dfs.getContentSummary(dir);
    Assert.assertEquals(spaceUsage, contentSummary.getSpaceConsumed());

    // Increasing replication from 3 to 4.
    short replication_4 = 4;
    dfs.setReplication(file, replication_4);
    assertEquals(replication_4, dfs.getFileStatus(file).getReplication());
    // Validate dir spaceConsumed will not update.
    quotaUsage = dfs.getQuotaUsage(dir);
    Assert.assertEquals(spaceUsage, quotaUsage.getSpaceConsumed());
    contentSummary = dfs.getContentSummary(dir);
    Assert.assertEquals(spaceUsage, contentSummary.getSpaceConsumed());

    // Decreasing replication from 4 to 3.
    dfs.setReplication(file, replication_3);
    assertEquals(replication_3, dfs.getFileStatus(file).getReplication());
    // Validate dir spaceConsumed will not update.
    quotaUsage = dfs.getQuotaUsage(dir);
    Assert.assertEquals(spaceUsage, quotaUsage.getSpaceConsumed());
    contentSummary = dfs.getContentSummary(dir);
    Assert.assertEquals(spaceUsage, contentSummary.getSpaceConsumed());

    // Increasing replication from 3 to 5.
    short replication_5 = 5;
    dfs.setReplication(file, replication_5);
    assertEquals(replication_5, dfs.getFileStatus(file).getReplication());
    // Validate dir spaceConsumed will not update.
    quotaUsage = dfs.getQuotaUsage(dir);
    Assert.assertEquals(spaceUsage, quotaUsage.getSpaceConsumed());
    contentSummary = dfs.getContentSummary(dir);
    Assert.assertEquals(spaceUsage, contentSummary.getSpaceConsumed());

    // Decreasing replication from 5 to 3.
    dfs.setReplication(file, replication_3);
    assertEquals(replication_3, dfs.getFileStatus(file).getReplication());
    // Validate dir spaceConsumed will not update.
    quotaUsage = dfs.getQuotaUsage(dir);
    Assert.assertEquals(spaceUsage, quotaUsage.getSpaceConsumed());
    contentSummary = dfs.getContentSummary(dir);
    Assert.assertEquals(spaceUsage, contentSummary.getSpaceConsumed());


    // Test Normal set replication from 3 to 2 and 2 to 3.
    // Create dir1 and set quota.
    Path dir1 = new Path("/testSetReplication1");
    dfs.mkdirs(dir1);
    dfs.setQuota(dir1, 100, DEFAULT_BLOCK_SIZE * 5 + 100);

    // Create 2 replication file.
    short replication_2 = 2;
    Path file1 = new Path("/testSetReplication1/file");
    DFSTestUtil.createFile(dfs, file1, DEFAULT_BLOCK_SIZE, replication_2, 0);
    // Validate dir1 spaceConsumed.
    QuotaUsage quotaUsage1 = dfs.getQuotaUsage(dir1);
    Assert.assertEquals(DEFAULT_BLOCK_SIZE * 2, quotaUsage1.getSpaceConsumed());
    ContentSummary contentSummary1 = dfs.getContentSummary(dir1);
    Assert.assertEquals(DEFAULT_BLOCK_SIZE * 2, contentSummary1.getSpaceConsumed());

    // Increasing replication from 2 to 3.
    dfs.setReplication(file1, replication_3);
    assertEquals(replication_3, dfs.getFileStatus(file1).getReplication());
    // Verification Increasing replication from 2 to 3, dir spaceConsumed will update.
    quotaUsage1 = dfs.getQuotaUsage(dir1);
    Assert.assertEquals(DEFAULT_BLOCK_SIZE * 3, quotaUsage1.getSpaceConsumed());
    contentSummary1 = dfs.getContentSummary(dir1);
    Assert.assertEquals(DEFAULT_BLOCK_SIZE * 3, contentSummary1.getSpaceConsumed());

    // Decreasing replication from 3 to 2.
    dfs.setReplication(file1, replication_2);
    assertEquals(replication_2, dfs.getFileStatus(file1).getReplication());
    // Verification Decreasing replication from 3 to 2, dir spaceConsumed will update.
    quotaUsage1 = dfs.getQuotaUsage(dir1);
    Assert.assertEquals(DEFAULT_BLOCK_SIZE * 2, quotaUsage1.getSpaceConsumed());
    contentSummary1 = dfs.getContentSummary(dir1);
    Assert.assertEquals(DEFAULT_BLOCK_SIZE * 2, contentSummary1.getSpaceConsumed());
  }

  private void scanDirsWithQuota(INodeDirectory dir,
      HashMap<String, Long> nsMap,
      HashMap<String, Long> dsMap, boolean verify) {
    if (dir.isQuotaSet()) {
      // get the current consumption
      QuotaCounts q = dir.getDirectoryWithQuotaFeature().getSpaceConsumed();
      String name = dir.getFullPathName();
      if (verify) {
        assertEquals(nsMap.get(name).longValue(), q.getNameSpace());
        assertEquals(dsMap.get(name).longValue(), q.getStorageSpace());
      } else {
        nsMap.put(name, q.getNameSpace());
        dsMap.put(name, q.getStorageSpace());
      }
    }

    for (INode child : dir.getChildrenList(Snapshot.CURRENT_STATE_ID)) {
      if (child instanceof INodeDirectory) {
        scanDirsWithQuota((INodeDirectory)child, nsMap, dsMap, verify);
      }
    }
  }

  private void updateCountForQuota(int i) {
    FSNamesystem fsn = cluster.getNamesystem();
    fsn.writeLock();
    try {
      cluster.getNamesystem(0).getFSDirectory().updateCountForQuota(i);
    } finally {
      fsn.writeUnlock();
    }
  }
}
