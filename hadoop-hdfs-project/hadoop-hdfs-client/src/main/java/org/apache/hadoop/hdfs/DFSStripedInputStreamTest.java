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

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.StripedRead.MOCK_FAILURE;

/**
 * This is one test class for DFSStripedInputStream.
 * DFSStripedInputStreamTest reads from striped block groups.
 */
public class DFSStripedInputStreamTest {
  public static final Logger LOG = LoggerFactory.getLogger(
      DFSStripedInputStreamTest.class);

  private final Configuration conf;

  public DFSStripedInputStreamTest() {
    this.conf = new HdfsConfiguration();
    this.conf.setBoolean("fs.hdfs.impl.disable.cache", true);
  }

  public void testECPolicyInfo(Path testPath)
      throws IOException {
    DistributedFileSystem dfs =
        (DistributedFileSystem) testPath.getFileSystem(this.conf);
    HdfsFileStatus hdfsFileStatus = dfs.getClient()
        .getFileInfo(testPath.toUri().getPath());
    ErasureCodingPolicy ecPolicy = hdfsFileStatus.getErasureCodingPolicy();
    if (ecPolicy == null) {
      throw new IOException("Can't get ec policy info for " + testPath
          + " by getFileInfo");
    }
    LOG.info("The ec policy in fileStatus is {} for {}, " +
        "and fileStatus is {}.", ecPolicy, testPath, hdfsFileStatus);

    ErasureCodingPolicy ecPolicy2 = dfs.getClient()
        .getErasureCodingPolicy(testPath.toUri().getPath());
    if (ecPolicy2 == null) {
      throw new IOException("Can't get ec policy info for "
          + testPath + " by getErasureCodingPolicy.");
    }
    LOG.info("The ec policy is {} for {} by getErasureCodingPolicy.",
        ecPolicy2, testPath);

    ContentSummary contentSummary = dfs.getClient()
        .getContentSummary(testPath.toUri().getPath());
    String ecPolicy3 = contentSummary.getErasureCodingPolicy();
    if (ecPolicy3 == null) {
      throw new IOException("Can't get ec policy info for "
          + testPath + " by getContentSummary.");
    }
    LOG.info("The ec policy is {} for {} by getContentSummary," +
            " and the contentSummary is {}.", ecPolicy2, testPath,
        contentSummary);
  }

  public void testReadECFile(Path testPath)
      throws IOException, NoSuchAlgorithmException {
    LOG.info("Will read ec file {} normally.", testPath);
    testReadECFile(this.conf, testPath);
  }

  public void testReadECFileWithDNFailure(Path testPath)
      throws IOException, NoSuchAlgorithmException {
    LOG.info("Will read ec file {} with mock missing.", testPath);
    Configuration copyConf = new Configuration(this.conf);
    copyConf.setBoolean(MOCK_FAILURE, true);
    testReadECFile(copyConf, testPath);
  }

  private void testReadECFile(Configuration conf, Path testPath)
      throws IOException, NoSuchAlgorithmException {
    DistributedFileSystem dfs =
        (DistributedFileSystem) testPath.getFileSystem(conf);
    FSDataInputStream fsDataInputStream = dfs.open(testPath);
    MessageDigest MD5 = MessageDigest.getInstance("MD5");

    byte[] buffer = new byte[8192];
    int length;
    while ((length = fsDataInputStream.read(buffer)) != -1) {
      MD5.update(buffer, 0, length);
    }
    String md5Result = new String(Hex.encodeHex(MD5.digest()));
    LOG.info("The md5Result is {} for {} by readWithStrategy.", md5Result, testPath);
    fsDataInputStream.close();

    fsDataInputStream = dfs.open(testPath);
    MessageDigest MD5WithPread = MessageDigest.getInstance("MD5");

    buffer = new byte[10 * 1024 * 1024];
    long position =  0;
    while ((length = fsDataInputStream.read(position, buffer, 0 , 8192)) != -1) {
      MD5WithPread.update(buffer, 0, length);
      position += length;
    }
    String md5ResultWithPread = new String(Hex.encodeHex(MD5WithPread.digest()));
    LOG.info("The md5Result is {} for {} by pread.", md5ResultWithPread, testPath);
    fsDataInputStream.close();
  }

  public static void main(String[] args) throws Exception {
    Path testPath = new Path(args[0]);
    DFSStripedInputStreamTest stripedInputStreamTest =
        new DFSStripedInputStreamTest();

    // Test EC Policy Info in HDFSFileStatus.
    stripedInputStreamTest.testECPolicyInfo(testPath);

    // Test read EC file.
    stripedInputStreamTest.testReadECFile(testPath);

    // Test read EC file with missing block.
    stripedInputStreamTest.testReadECFileWithDNFailure(testPath);
  }
}