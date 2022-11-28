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
package org.apache.hadoop.tools.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.tools.DFSck;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.EnumSet;
import java.util.Random;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;
import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.LAZY_PERSIST;
import static org.apache.hadoop.fs.CreateFlag.OVERWRITE;

public class ShopeeDFSUtil {
  public static void createFile(FileSystem fs, Path fileName, long fileLen)
      throws IOException {
    createFile(fs, fileName, 1024 * 1024, fileLen, fs.getDefaultBlockSize(fileName),
        fs.getDefaultReplication(fileName), 1);
  }

  public static void createFile(FileSystem fs, Path fileName, long fileLen,
      short replFactor, long seed) throws IOException {
    createFile(fs, fileName, 1024, fileLen, fs.getDefaultBlockSize(fileName),
        replFactor, seed);
  }

  public static void createFile(FileSystem fs, Path fileName, int bufferLen,
      long fileLen, long blockSize, short replFactor, long seed) throws IOException {
    createFile(fs, fileName, false, bufferLen, fileLen, blockSize, replFactor,
        seed, false);
  }

  public static void createFile(FileSystem fs, Path fileName,
      boolean isLazyPersist, int bufferLen, long fileLen, long blockSize,
      short replFactor, long seed, boolean flush) throws IOException {
    createFile(fs, fileName, isLazyPersist, bufferLen, fileLen, blockSize,
        replFactor, seed, flush, null);
  }

  public static void createFile(FileSystem fs, Path fileName,
      boolean isLazyPersist, int bufferLen, long fileLen, long blockSize,
      short replFactor, long seed, boolean flush,
      InetSocketAddress[] favoredNodes) throws IOException {
    assert bufferLen > 0;
    if (!fs.mkdirs(fileName.getParent())) {
      throw new IOException("Mkdirs failed to create " +
          fileName.getParent().toString());
    }
    EnumSet<CreateFlag> createFlags = EnumSet.of(CREATE);
    createFlags.add(OVERWRITE);
    if (isLazyPersist) {
      createFlags.add(LAZY_PERSIST);
    }
    try (FSDataOutputStream out = (favoredNodes == null) ?
        fs.create(fileName, FsPermission.getFileDefault(), createFlags,
            fs.getConf().getInt(IO_FILE_BUFFER_SIZE_KEY, 4096), replFactor,
            blockSize, null)
        :
        ((DistributedFileSystem) fs).create(fileName, FsPermission.getDefault(),
            true, bufferLen, replFactor, blockSize, null, favoredNodes)
    ) {
      writeData(out, bufferLen, fileLen, seed, flush);
    }
  }

  private static void writeData(FSDataOutputStream out, int bufferLen,
      long fileLen, long seed, boolean flush) throws IOException {
    if (fileLen > 0) {
      byte[] toWrite = new byte[bufferLen];
      Random rb = new Random(seed);
      long bytesToWrite = fileLen;
      while (bytesToWrite > 0) {
        rb.nextBytes(toWrite);
        int bytesToWriteNext = (bufferLen < bytesToWrite) ? bufferLen : (int) bytesToWrite;
        out.write(toWrite, 0, bytesToWriteNext);
        bytesToWrite -= bytesToWriteNext;

        if (flush) {
          out.hsync();
        }
      }
    }
  }

  public static void append(FileSystem fs, Path fileName, long fileLen) throws IOException {
    append(fs, fileName, 1024 * 1024, fileLen, 1);
  }

  public static void append(FileSystem fs, Path fileName, int bufferLen, long fileLen, long seed)
      throws IOException {
    try (FSDataOutputStream out = fs.append(fileName)) {
      writeData(out, bufferLen, fileLen, seed, true);
    }
  }

  public static boolean setReplication(FileSystem fs, Path path, short targetReplication)
      throws IOException {
    return fs.setReplication(path, targetReplication);
  }

  public static void setTimes(FileSystem fs, Path path, long modifyTime, long accessTime)
      throws IOException {
    fs.setTimes(path, modifyTime, accessTime);
  }

  public static void setPermission(FileSystem fs, Path path, FsPermission targetPermission)
      throws IOException {
    fs.setPermission(path, targetPermission);
  }

  public static void setOwner(FileSystem fs, Path path, String owner, String group)
      throws IOException {
    fs.setOwner(path, owner, group);
  }

  public static boolean truncate(FileSystem fs, Path path, long targetLength) throws IOException {
    return fs.truncate(path, targetLength);
  }

  public static boolean rename(FileSystem fs, Path srcPath, Path targetPath) throws IOException {
    return fs.rename(srcPath, targetPath);
  }

  public static boolean delete(FileSystem fs, Path path) throws IOException {
    return fs.delete(path, true);
  }

  public static void setStoragePolicy(FileSystem fs, Path path, String policyName)
      throws IOException {
    fs.setStoragePolicy(path, policyName);
  }

  public static BlockLocation[] getBlockLocations(FileSystem fs,
      Path path, long offset, long length) throws IOException {
    return fs.getFileBlockLocations(path, offset, length);
  }

  public static FileStatus getFileStatus(FileSystem fs, Path path) throws IOException {
    return fs.getFileStatus(path);
  }

  public static boolean isExisted(FileSystem fs, Path path) throws IOException {
    return fs.exists(path);
  }

  public static FileStatus[] listStatus(FileSystem fs, Path path) throws IOException {
    return fs.listStatus(path);
  }

  public static ContentSummary contentSummary(FileSystem fs, Path path) throws IOException {
    return fs.getContentSummary(path);
  }

  public static boolean isFileClosed(FileSystem fs, Path path) throws IOException {
    return fs.isFile(path);
  }

  public static void setQuota(FileSystem fs, Path path, long namespaceQuota, long storageSpaceQuota)
      throws IOException {
    fs.setQuota(path, namespaceQuota, storageSpaceQuota);
  }

  public static QuotaUsage quotaUsage(FileSystem fs, Path path) throws IOException {
    return fs.getQuotaUsage(path);
  }

  public static AclStatus aclStatus(FileSystem fs, Path path) throws IOException {
    return fs.getAclStatus(path);
  }

  public static int fsck(Configuration conf, String... path) throws Exception {
    return ToolRunner.run(new DFSck(conf), path);
  }
}
