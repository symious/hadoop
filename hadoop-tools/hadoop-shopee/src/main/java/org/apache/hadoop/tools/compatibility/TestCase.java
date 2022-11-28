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
package org.apache.hadoop.tools.compatibility;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.tools.common.ShopeeDFSUtil;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A class for testing compatibility between different hadoop versions.
 */
public class TestCase {
  private static final Logger LOG = LoggerFactory.getLogger(TestCase.class);
  private static final SimpleDateFormat dateFormat = new SimpleDateFormat(("yyyy-MM-dd"));

  private final Path rootPath = new Path("/tmp/hdfs-compatibility");
  private final FileSystem fileSystem;
  private final ExecutorService executorService = HadoopExecutors.newFixedThreadPool(100);
  private final boolean enableQuota;
  private final long fileLength;

  public TestCase(Configuration conf, Path namespaceSchema, boolean enableQuota, long fileLength)
      throws IOException {
    this.fileSystem = namespaceSchema.getFileSystem(conf);
    this.enableQuota = enableQuota;
    this.fileLength = fileLength;
  }

  public void testWriteOperations() throws IOException {
    Path directory = new Path(rootPath, (dateFormat.format(new Date()) + "-" +
        ThreadLocalRandom.current().nextLong() + "-" + UUID.randomUUID()));
    if (!fileSystem.exists(directory)) {
      fileSystem.mkdirs(directory);
    }

    if (enableQuota) {
      // SetQuota
      ShopeeDFSUtil.setQuota(fileSystem, directory, 1024 * 1024, 1024 * 1024 * 1024 * 10L);
    }

    Path testFile = new Path(directory, "file-" + ThreadLocalRandom.current().nextLong());

    // Create, addBlockLocation, Complete.
    ShopeeDFSUtil.createFile(fileSystem, testFile, fileLength);

    // SetReplication
    boolean setReplicationResult = ShopeeDFSUtil.setReplication(fileSystem, testFile, (short) 2);
    LOG.debug("The result of setReplication is {} for {}.", setReplicationResult, testFile);

    // SetTimes
    ShopeeDFSUtil.setTimes(fileSystem, testFile, Time.monotonicNow(), Time.monotonicNow());

    // SetStoragePolicy
    ShopeeDFSUtil.setStoragePolicy(fileSystem, testFile, "HOT");

    // SetPermission
    ShopeeDFSUtil.setPermission(fileSystem, testFile, FsPermission.getCachePoolDefault());

    // SetOwner, Tips: the running ugi should be superuser, such as hdfs.
    ShopeeDFSUtil.setOwner(fileSystem, testFile, "mock_user", "mock_group");

    if (!ShopeeDFSUtil.getFileStatus(fileSystem, testFile).getOwner().equals("mock_user")) {
      String message = "The owner of " + testFile + " is "
          + ShopeeDFSUtil.getFileStatus(fileSystem, testFile)
          + ", it should be mock_user, please verify it.";
      throw new IOException(message);
    }

    // Append
    ShopeeDFSUtil.append(fileSystem, testFile, fileLength);

    // Truncate.
    long targetLength = fileLength > 0 ? (long)Math.ceil(fileLength / 2.0) : 0;
    if (targetLength > 0) {
      boolean truncateResult = ShopeeDFSUtil.truncate(fileSystem, testFile, targetLength);
      LOG.debug("The result of truncate is {} for {}.", truncateResult, testFile);
    }

    Path targetFile = new Path(directory, "file-" + ThreadLocalRandom.current().nextLong());

    // Rename
    boolean renameResult = ShopeeDFSUtil.rename(fileSystem, testFile, targetFile);
    LOG.debug("The result of rename is {} from {} to {}.", renameResult, testFile, targetFile);

    if (ShopeeDFSUtil.isExisted(fileSystem, testFile)) {
      String message = "The file " + testFile + " should be null, please verify it.";
      throw new IOException(message);
    }

    if (!ShopeeDFSUtil.isExisted(fileSystem, targetFile)) {
      String message = "The file " + targetFile + " shouldn't be null, please verify it.";
      throw new IOException(message);
    }

    // Delete
    boolean deleteResult = ShopeeDFSUtil.delete(fileSystem, targetFile);
    LOG.debug("The result of delete is {} for {}.", deleteResult, targetFile);

    if (ShopeeDFSUtil.isExisted(fileSystem, targetFile)) {
      String message = "The file " + targetFile + " should be null, please verify it.";
      throw new IOException(message);
    }

    // Delete RootPath
    ShopeeDFSUtil.delete(fileSystem, directory);
  }

  public void testReadOperations() throws Exception {
    Path directory = new Path(rootPath, (dateFormat.format(new Date()) + "-" +
        ThreadLocalRandom.current().nextLong() + "-" + UUID.randomUUID()));
    if (!fileSystem.exists(directory)) {
      fileSystem.mkdirs(directory);
    }

    if (this.enableQuota) {
      // SetQuota
      ShopeeDFSUtil.setQuota(fileSystem, directory, 1024 * 1024, 1024 * 1024 * 1024 * 10L);
    }

    Path testFile = new Path(directory, "file-" + ThreadLocalRandom.current().nextLong());

    // Create, addBlockLocation, Complete.
    ShopeeDFSUtil.createFile(fileSystem, testFile, 1024 * 1024);

    // GetBlockLocations
    ShopeeDFSUtil.getBlockLocations(fileSystem, testFile, 0, Long.MAX_VALUE);

    // GetFileStatus
    ShopeeDFSUtil.getFileStatus(fileSystem, testFile);

    // ListStatus
    ShopeeDFSUtil.listStatus(fileSystem, directory);

    // ContentSummary
    ShopeeDFSUtil.contentSummary(fileSystem, directory);

    // IsFileClosed
    ShopeeDFSUtil.isFileClosed(fileSystem, testFile);

    if (this.enableQuota) {
      // QuotaUsage
      ShopeeDFSUtil.quotaUsage(fileSystem, testFile);
    }

    // AclStatus
    ShopeeDFSUtil.aclStatus(fileSystem, testFile);

    // Delete RootPath
    ShopeeDFSUtil.delete(fileSystem, directory);
  }

  public ArrayList<Future<Boolean>> multiThreadTesting(int threadNumber, long loopNumber) {
    ArrayList<Future<Boolean>> futures = new ArrayList<>();
    for (int i = 0; i < threadNumber; i++) {
      Future<Boolean> future = executorService.submit(() -> {
        int loopIndex = 0;
        while (loopNumber == -1 || loopIndex++ <= loopNumber) {
          testWriteOperations();
          testReadOperations();
        }
        return true;
      });
      futures.add(future);
    }
    return futures;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new HdfsConfiguration();// NameSpace
    Path namespaceSchema = new Path(args[0]);
    int threadNumber = Integer.parseInt(args[1]);
    long loopNumber = Long.parseLong(args[2]);
    boolean enableQuota = Boolean.parseBoolean(args[3]);
    long fileLength = Long.parseLong(args[4]);

    TestCase testCase = new TestCase(conf, namespaceSchema, enableQuota, fileLength);
    ArrayList<Future<Boolean>> futures = testCase.multiThreadTesting(threadNumber, loopNumber);
    for (Future<Boolean> future : futures) {
      try {
        future.get();
      } catch (Throwable e) {
        LOG.warn("Failed. ", e);
      }
    }
    LOG.info("TestCase finished with parameters {}.", Arrays.toString(args));
    System.exit(0);
  }
}
