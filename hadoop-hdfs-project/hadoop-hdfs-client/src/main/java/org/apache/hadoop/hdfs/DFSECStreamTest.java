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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CHECKSUM_COMBINE_MODE_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.StripedRead.MOCK_FAILURE;

/**
 * A test class for DFSStripedInputStream and DFSStripedOutputStream.
 */
public class DFSECStreamTest {
  public static final Logger LOG = LoggerFactory.getLogger(DFSECStreamTest.class);
  private static final int DEFAULT_BATCH_SIZE = 10 * 1024 * 1024;
  private static int FIXED_DATA_GENERATOR = 0;

  private final Configuration normalConf;
  private final Configuration mockMissingConf;

  public DFSECStreamTest() {
    this.normalConf = new HdfsConfiguration();
    this.normalConf.setBoolean("fs.hdfs.impl.disable.cache", true);

    Configuration mockConf = new Configuration(this.normalConf);
    mockConf.setBoolean(MOCK_FAILURE, true);
    this.mockMissingConf = mockConf;
  }

  /**
   * Generate some random data.
   */
  protected byte[] generateRandomData(int len) {
    byte[] buffer = new byte[len];
    for (int i = 0; i < buffer.length; i++) {
      buffer[i] = (byte) ThreadLocalRandom.current().nextInt(256);
    }
    return buffer;
  }

  /**
   * Generate some fixed data.
   */
  protected byte[] generateFixedData(int len) {
    byte[] buffer = new byte[len];
    for (int i = 0; i < buffer.length; i++) {
      buffer[i] = (byte) FIXED_DATA_GENERATOR++;
      if (FIXED_DATA_GENERATOR == 256) {
        FIXED_DATA_GENERATOR = 0;
      }
    }
    return buffer;
  }

  /**
   * Create one EC file with the EC PolicyName, and fill it with the random data.
   * @return md5 of this file.
   */
  private String createOneECFile(Path ecFilePath,
      String ecPolicyName, long fileLength, boolean useRandomData)
      throws IOException, NoSuchAlgorithmException {
    LOG.info("Will create one EC file {} with ecPolicy {}, and file size is {}.",
        ecFilePath, ecPolicyName, fileLength);
    DistributedFileSystem dfs = (DistributedFileSystem) ecFilePath
        .getFileSystem(this.normalConf);
    DistributedFileSystem.HdfsDataOutputStreamBuilder builder
        = dfs.createFile(ecFilePath);
    builder.ecPolicyName(ecPolicyName);
    FSDataOutputStream fsDataOutputStream = builder.build();
    MessageDigest md5 = MessageDigest.getInstance("MD5");
    int writeLength = 0;
    while (writeLength < fileLength) {
      int currentBatchSize =
          (int) Math.min(DEFAULT_BATCH_SIZE, fileLength - writeLength);
      byte[] generatedData = useRandomData ? generateRandomData(currentBatchSize)
          : generateFixedData(currentBatchSize);
      fsDataOutputStream.write(generatedData);
      md5.update(generatedData, 0, currentBatchSize);
      writeLength += currentBatchSize;
    }

    fsDataOutputStream.close();

    String md5Value = Hex.encodeHexString(md5.digest());
    LOG.info("Successfully create one EC file {} with ecPolicy {}, " +
            "and file size is {}, and md5 is {}.",
        ecFilePath, ecPolicyName, fileLength, md5Value);
    return md5Value;
  }

  /**
   * Read some data from the ecFile by ReadStrategy, and return md5.
   * @param useMockMissing whether to use mock MissingException during reading.
   * @return MD5 of the contents.
   */
  private String readWithStrategyRead(Path ecFilePath,
      long startOffset, long needReadLength, boolean useMockMissing)
      throws IOException, NoSuchAlgorithmException {
    LOG.info("Will read {}(bytes) data started with {} from {} by StrategyRead, " +
            "and mockMissing flag is {}.",
        needReadLength, startOffset, ecFilePath, useMockMissing);
    MessageDigest md5 = MessageDigest.getInstance("MD5");
    DFSInputStream dfsInputStream = getDFSInputStream(ecFilePath,
        startOffset, needReadLength, useMockMissing);
    dfsInputStream.seek(startOffset);

    long alreadyReadLength = 0;
    while (alreadyReadLength < needReadLength) {
      int bufferSize = (int) Math.min(DEFAULT_BATCH_SIZE,
          needReadLength - alreadyReadLength);
      byte[] buffer = new byte[bufferSize];
      int size = dfsInputStream.read(buffer);
      if (size == -1) {
        throw new IOException("UnExpected EOF happened, please check it.");
      }
      md5.update(buffer, 0 , size);
      alreadyReadLength += size;
    }
    dfsInputStream.close();
    String md5Value = Hex.encodeHexString(md5.digest());
    LOG.info("Successfully read {}(bytes) data started with {} from {} by StrategyRead, " +
            "and mockMissing flag is {}, and md5Value is {}.",
        needReadLength, startOffset, ecFilePath, useMockMissing, md5Value);
    return md5Value;
  }

  /**
   * Read some data from the ecFile by ReadStrategy, and return md5.
   * @param useMockMissing whether to use mock MissingException during reading.
   * @return MD5 of the contents.
   */
  private String readWithPRead(Path ecFilePath,
      long startOffset, long needReadLength, boolean useMockMissing)
      throws IOException, NoSuchAlgorithmException {
    LOG.info("Will read {}(bytes) data started with {} from {} by PRead, " +
            "and mockMissing flag is {}.",
        needReadLength, startOffset, ecFilePath, useMockMissing);
    DFSInputStream dfsInputStream = getDFSInputStream(ecFilePath,
        startOffset, needReadLength, useMockMissing);
    MessageDigest md5 = MessageDigest.getInstance("MD5");

    long alreadyReadLength = 0;
    while (alreadyReadLength < needReadLength) {
      int bufferSize = (int) Math.min(DEFAULT_BATCH_SIZE,
          needReadLength - alreadyReadLength);
      byte[] buffer = new byte[bufferSize];
      int size = dfsInputStream.read(
          startOffset + alreadyReadLength, buffer, 0, bufferSize);
      if (size == -1) {
        throw new IOException("UnExpected EOF happened, please check it.");
      }
      md5.update(buffer, 0 , size);
      alreadyReadLength += size;
    }
    dfsInputStream.close();
    String md5Value = Hex.encodeHexString(md5.digest());
    LOG.info("Successfully read {}(bytes) data started with {} from {} by PRead, " +
            "and mockMissing flag is {}, and md5Value is {}.",
        needReadLength, startOffset, ecFilePath, useMockMissing, md5Value);
    return md5Value;
  }

  private DFSInputStream getDFSInputStream(Path ecFilePath,
      long startOffset, long needReadLength, boolean useMockMissing)
      throws IOException {
    Configuration conf = useMockMissing ? this.mockMissingConf : this.normalConf;
    DistributedFileSystem dfs =
        (DistributedFileSystem) ecFilePath.getFileSystem(conf);
    DFSInputStream dfsInputStream = dfs.getClient().open(
        ecFilePath.toUri().getPath());
    if (startOffset + needReadLength > dfsInputStream.getFileLength()) {
      throw new IOException((startOffset + needReadLength)
          + " is greater than file size " + dfsInputStream.getFileLength());
    }
    return dfsInputStream;
  }

  /**
   * Create one EC file and fill it with data of a specific length.
   * And then verify the correctness of the data through a variety of reading method.
   *
   * @param ecPolicyName ec policy name
   * @param fileSize the specific file length.
   * @param useRandomData if true, will use random data, else use fixed data.
   */
  public void testReadECFileWithCreate(Path testPath,
      String ecPolicyName, long fileSize, boolean useRandomData)
      throws IOException, NoSuchAlgorithmException {
    String md5 = createOneECFile(testPath, ecPolicyName,
        fileSize, useRandomData);
    String readMD5 = testReadPath(testPath, 0, fileSize);
    if (!readMD5.equals(md5)) {
      throw new IOException("There are some error happened, " +
          "please check it through logs.");
    }
    deleteTestPath(testPath);
  }

  private void deleteTestPath(Path testPath) throws IOException {
    FileSystem fs = testPath.getFileSystem(this.normalConf);
    fs.delete(testPath, true);
  }

  /**
   * Verify the correctness of the data from the EC file through a variety of reading method.
   * @param expectedMD5 the expected md5 of the data.
   */
  public void testReadExistedECFile(Path testPath, String expectedMD5)
      throws IOException, NoSuchAlgorithmException {
    FileSystem fileSystem = testPath.getFileSystem(this.normalConf);
    long fileSize = fileSystem.getFileStatus(testPath).getLen();
    String readMD5 = testReadPath(testPath, 0 , fileSize);
    if (!readMD5.equals(expectedMD5)) {
      throw new IOException("There are some errors happened, please check it though logs");
    }
  }

  private String testReadPath(Path testPath, long startOffset, long fileSize)
      throws IOException, NoSuchAlgorithmException {
    String normalStrategyMD5 = readWithStrategyRead(
        testPath, startOffset, fileSize, false);
    String normalPReadMD5 = readWithPRead(
        testPath, startOffset, fileSize, false);
    String missingStrategyMD5 = readWithStrategyRead(
        testPath, startOffset, fileSize, true);
    String missingPReadMD5 = readWithPRead(
        testPath, startOffset, fileSize, true);
    LOG.info("TestReadExistedECFile TestPath is {}, FileSize is {}, " +
            "normalStrategyMD5 is {}, normalPReadMD5 is {}," +
            " missingStrategyMD5 is {}, missingPReadMD5 is {}",
        testPath, fileSize, normalStrategyMD5, normalPReadMD5,
        missingStrategyMD5, missingPReadMD5);
    if (!normalPReadMD5.equals(normalStrategyMD5)
        || !missingStrategyMD5.equals(normalStrategyMD5)
        || !missingPReadMD5.equals(normalStrategyMD5)) {
      throw new IOException("There are some errors happened, please check it though logs");
    }
    return normalStrategyMD5;
  }

  /**
   * Verify the read process with random offset and length.
   * @param existedTestPath existed file path which is not EC file.
   * @param testTmpPath the temperate file path for testing.
   * @param expectedLoopNum expected verification rounds
   *
   * @throws IOException throw IOException if some errors happens.
   */
  public void testReadWithRandomOffsetAndLength(Path existedTestPath,
      Path testTmpPath, String ecPolicyName, int expectedLoopNum)
      throws IOException, NoSuchAlgorithmException {

    // Step1: read data from existedTestPath and write into testTmpPath;
    DistributedFileSystem dfs = (DistributedFileSystem)
        existedTestPath.getFileSystem(this.normalConf);
    long fileLength = dfs.getFileStatus(existedTestPath).getLen();
    String expectedMD5 = writeECFileWithNormalFile(dfs,
        existedTestPath, testTmpPath, ecPolicyName);

    // Step2: read data from testTmpPath to verify md5
    testReadExistedECFile(testTmpPath, expectedMD5);

    // Step3: read data from existedTestPath and testTmpPath with random offset and length
    int loopNum = 0;
    while (loopNum++ < expectedLoopNum) {
      // Step3.1 random length and offset.
      long randomLength = ThreadLocalRandom.current()
          .nextLong(fileLength - 10 * 1024 * 1024);
      long randomOffset = ThreadLocalRandom.current()
          .nextLong(fileLength - randomLength);

      // Step3.2 read from normal file and compute MD5
      String expectMD5 = testReadPath(existedTestPath, randomOffset, randomLength);

      // Step3.3 read from ec file and compute MD5
      String readMD5 = testReadPath(testTmpPath, randomOffset, randomLength);

      if (!readMD5.equals(expectMD5)) {
        throw new IOException("There are some errors happened, please check it.");
      }
    }
    deleteTestPath(testTmpPath);
  }

  /**
   * Create one ec file with ecPolicyName and
   * transfer the data of existed file to it.
   *
   * @return the md5 of transmitted data.
   */
  private String writeECFileWithNormalFile(DistributedFileSystem dfs,
      Path normalFile, Path ecFile, String ecPolicyName)
      throws IOException, NoSuchAlgorithmException {
    LOG.info("Will create and write one ec file {} with policy {} from {}.",
        ecFile, ecPolicyName, normalFile);
    FSDataInputStream fsDataInputStream = dfs.open(normalFile);
    DistributedFileSystem.HdfsDataOutputStreamBuilder builder
        = dfs.createFile(ecFile);
    builder.ecPolicyName(ecPolicyName);
    FSDataOutputStream fsDataOutputStream = builder.build();
    MessageDigest md5 = MessageDigest.getInstance("MD5");

    int transferSize;
    byte[] transferBuffer = new byte[DEFAULT_BATCH_SIZE];
    while ((transferSize = fsDataInputStream.read(transferBuffer)) != -1)  {
      fsDataOutputStream.write(transferBuffer, 0, transferSize);
      md5.update(transferBuffer, 0, transferSize);
    }
    fsDataInputStream.close();
    fsDataOutputStream.close();

    String md5Value = Hex.encodeHexString(md5.digest());
    LOG.info("Successfully create and write one ec file {} " +
            "with policy {} from {}, and md5 is {}",
        ecFile, ecPolicyName, normalFile, md5Value);
    return md5Value;

  }

  /**
   * Get the ECPolicy information for EC file through various methods,
   * link getFileInfo, getErasureCodingPolicy, getContentSummary.
   */
  private void testECPolicyInfo(Path testPath) throws IOException {
    DistributedFileSystem dfs =
        (DistributedFileSystem) testPath.getFileSystem(this.normalConf);
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

  /**
   * Test Write EC File with full stripe.
   */
  private void testWriteCornerCase1(Path testPath)
      throws IOException, NoSuchAlgorithmException {
    String ecPolicyName = "RS-6-3-1024k";
    long cellSize = 1024 * 1024;
    long stripeSize = 6 * cellSize;
    long fileLength = 10 * stripeSize;
    testReadECFileWithCreate(testPath, ecPolicyName, fileLength, true);
    LOG.info("testWriteCornerCase1 success.");
  }

  /**
   * Test Write EC File with partial stripe, and full cell.
   */
  private void testWriteCornerCase2(Path testPath)
      throws IOException, NoSuchAlgorithmException {
    String ecPolicyName = "RS-6-3-1024k";
    long cellSize = 1024 * 1024;
    long stripeSize = 6 * cellSize;
    long fileLength = 10 * stripeSize + 2 * cellSize;
    testReadECFileWithCreate(testPath, ecPolicyName, fileLength, true);
    LOG.info("testWriteCornerCase2 success.");
  }

  /**
   * Test Write EC File with only one partial stripe, and full cell.
   */
  private void testWriteCornerCase3(Path testPath)
      throws IOException, NoSuchAlgorithmException {
    String ecPolicyName = "RS-6-3-1024k";
    long cellSize = 1024 * 1024;
    long fileLength = 3 * cellSize;
    testReadECFileWithCreate(testPath, ecPolicyName, fileLength, true);
    LOG.info("testWriteCornerCase3 success.");
  }

  /**
   * Test Write EC File with partial stripe, and partial cell.
   */
  private void testWriteCornerCase4(Path testPath)
      throws IOException, NoSuchAlgorithmException {
    String ecPolicyName = "RS-6-3-1024k";
    long cellSize = 1024 * 1024;
    long stripeSize = 6 * cellSize;
    long fileLength = 10 * stripeSize + 2 * cellSize + (long) (0.8765 * cellSize);
    testReadECFileWithCreate(testPath, ecPolicyName, fileLength, true);
    LOG.info("testWriteCornerCase4 success.");
  }

  /**
   * Test Write EC File with only one partial stripe, and partial cell.
   */
  private void testWriteCornerCase5(Path testPath)
      throws IOException, NoSuchAlgorithmException {
    String ecPolicyName = "RS-6-3-1024k";
    long cellSize = 1024 * 1024;
    long fileLength = 4 * cellSize + (long) (0.8765 * cellSize);
    testReadECFileWithCreate(testPath, ecPolicyName, fileLength, true);
    LOG.info("testWriteCornerCase5 success.");
  }

  /**
   * Test Write EC File with only one partial cell.
   */
  private void testWriteCornerCase6(Path testPath)
      throws IOException, NoSuchAlgorithmException {
    String ecPolicyName = "RS-6-3-1024k";
    long cellSize = 1024 * 1024;
    long fileLength = (long) (0.8765 * cellSize);
    testReadECFileWithCreate(testPath, ecPolicyName, fileLength, true);
    LOG.info("testWriteCornerCase6 success.");
  }

  /**
   * Test read one EC file with partial stripe and full cell.
   */
  private void testReadCornerCase1(Path testPath)
      throws IOException, NoSuchAlgorithmException {
    String ecPolicyName = "RS-6-3-1024k";
    long cellSize = 1024 * 1024;
    long stripeSize = 6 * cellSize;
    long fileLength = stripeSize + 3 * cellSize;
    String newFileMD5 = createOneECFile(testPath,
        ecPolicyName, fileLength, true);
    testReadExistedECFile(testPath, newFileMD5);

    long cornerOffset = cellSize + (long) (0.8 * cellSize);
    long cornerLength = fileLength - cornerOffset - (long) (0.3 * cellSize);
    loopReadFile(testPath, cornerOffset, cornerLength, 11);
    deleteTestPath(testPath);
    LOG.info("testReadCornerCase1 success.");
  }

  private void loopReadFile(Path testPath, long startOffset,
      long readLength, int expectedLoopNum)
      throws IOException, NoSuchAlgorithmException {
    String readMD5 = testReadPath(testPath, startOffset, readLength);

    int loopNum = 0;
    while (loopNum++ < expectedLoopNum) {
      String currentReadMD5 = testReadPath(testPath, startOffset, readLength);
      if (!currentReadMD5.equals(readMD5)) {
        throw new IOException("There are some errors happened, please check it.");
      }
    }
  }

  /**
   * Test read one EC file with partial stripe and partial cell.
   */
  private void testReadCornerCase2(Path testPath)
      throws IOException, NoSuchAlgorithmException {
    String ecPolicyName = "RS-6-3-1024k";
    long cellSize = 1024 * 1024;
    long stripeSize = 6 * cellSize;
    long partialCellSie = (long) (0.5 * cellSize);
    long fileLength = stripeSize + 3 * cellSize + partialCellSie;
    String newFileMD5 = createOneECFile(testPath,
        ecPolicyName, fileLength, true);
    testReadExistedECFile(testPath, newFileMD5);

    long cornerOffset = cellSize + (long) (0.3 * cellSize);
    long cornerLength = fileLength - cornerOffset - partialCellSie - (long) (0.3 * cellSize);
    loopReadFile(testPath, cornerOffset, cornerLength, 10);
    deleteTestPath(testPath);
    LOG.info("testReadCornerCase2 success.");
  }

  /**
   * Test read one EC file with partial stripe and partial cell.
   */
  private void testReadCornerCase3(Path testPath)
      throws IOException, NoSuchAlgorithmException {
    String ecPolicyName = "RS-6-3-1024k";
    long cellSize = 1024 * 1024;
    long stripeSize = 6 * cellSize;
    long partialCellSie = (long) (0.8 * cellSize);
    long fileLength = stripeSize + 3 * cellSize + partialCellSie;
    String newFileMD5 = createOneECFile(testPath,
        ecPolicyName, fileLength, true);
    testReadExistedECFile(testPath, newFileMD5);

    long cornerOffset = cellSize + (long) (0.3 * cellSize);
    long cornerLength = fileLength - cornerOffset - partialCellSie - (long) (0.5 * cellSize);
    loopReadFile(testPath, cornerOffset, cornerLength, 10);
    deleteTestPath(testPath);
    LOG.info("testReadCornerCase3 success.");
  }

  /**
   * Test read one EC file with one full stripe and one partial cell.
   */
  private void testReadCornerCase4(Path testPath)
      throws IOException, NoSuchAlgorithmException {
    String ecPolicyName = "RS-6-3-1024k";
    long cellSize = 1024 * 1024;
    long stripeSize = 6 * cellSize;
    long partialCellSie = (long) (0.8 * cellSize);
    long fileLength = stripeSize + partialCellSie;
    String newFileMD5 = createOneECFile(testPath,
        ecPolicyName, fileLength, true);
    testReadExistedECFile(testPath, newFileMD5);

    long cornerOffset = cellSize + (long) (0.3 * cellSize);
    long cornerLength = fileLength - cornerOffset - (long) (0.2 * partialCellSie);
    loopReadFile(testPath, cornerOffset, cornerLength, 10);
    deleteTestPath(testPath);
    LOG.info("testReadCornerCase4 success.");
  }

  /**
   * Test read one EC file with only one partial cell.
   */
  private void testReadCornerCase5(Path testPath)
      throws IOException, NoSuchAlgorithmException {
    String ecPolicyName = "RS-6-3-1024k";
    int cellSize = 1024 * 1024;
    int partialCellSie = (int) (0.8 * cellSize);
    String newFileMD5 = createOneECFile(testPath,
        ecPolicyName, partialCellSie, true);
    testReadExistedECFile(testPath, newFileMD5);

    int cornerOffset = (int) (0.1 * partialCellSie);
    int cornerLength = partialCellSie - cornerOffset - (int) (0.1 * partialCellSie);
    loopReadFile(testPath, cornerOffset, cornerLength, 10);
    deleteTestPath(testPath);

    LOG.info("testReadCornerCase5 success.");
  }

  private void testReadECFileWithCornerCase(Path testPath)
      throws IOException, NoSuchAlgorithmException {
    testReadCornerCase1(testPath);
    testReadCornerCase2(testPath);
    testReadCornerCase3(testPath);
    testReadCornerCase4(testPath);
    testReadCornerCase5(testPath);
  }

  private void testWriteECFileWithCornerCase(Path testPath)
      throws IOException, NoSuchAlgorithmException {
    testWriteCornerCase1(testPath);
    testWriteCornerCase2(testPath);
    testWriteCornerCase3(testPath);
    testWriteCornerCase4(testPath);
    testWriteCornerCase5(testPath);
    testWriteCornerCase6(testPath);
  }

  private long readFile(FileSystem fs, Path fileName)
      throws IOException {
    long totalNumber = 0;
    try (FSDataInputStream in = fs.open(fileName)) {
      byte[] buf = new byte[64 * 1024];
      int bytesRead = in.read(buf);
      while (bytesRead >= 0) {
        totalNumber += bytesRead;
        bytesRead = in.read(buf);
      }
    }
    return totalNumber;
  }

  private void testCompositeCrc(Path replicateFilePath,
      Path ecFilePath, boolean useCache, int loopNumber) throws IOException {
    LOG.info("Will verify replicateFilePath {} and ecFilePath {}, " +
        "and useCache is {}.", replicateFilePath, ecFilePath, useCache);
    Configuration compositeConf = new Configuration(this.normalConf);
    compositeConf.set(DFS_CHECKSUM_COMBINE_MODE_KEY,
        Options.ChecksumCombineMode.COMPOSITE_CRC.name());

    FileSystem replicaFS = replicateFilePath.getFileSystem(compositeConf);
    FileSystem ecFS = ecFilePath.getFileSystem(compositeConf);
    long fileLength = replicaFS.getFileStatus(replicateFilePath).getLen();

    FileChecksum replicaFullCK = replicaFS.getFileChecksum(replicateFilePath);
    FileChecksum ecFullCK = ecFS.getFileChecksum(ecFilePath);
    if (!replicaFullCK.equals(ecFullCK)) {
      throw new IOException("EcFullCK not equal with ReplicaFullCK");
    } else {
      LOG.info("ReplicaFullCK {} is same with ECFullCK {}.", replicaFullCK, ecFullCK);
    }

    replicaFS.setNeedComputeCompositeCrc(useCache);
    ecFS.setNeedComputeCompositeCrc(useCache);

    long replicaFileLength = readFile(replicaFS, replicateFilePath);
    long ecFileLength = readFile(ecFS, ecFilePath);
    if (fileLength != replicaFileLength || fileLength != ecFileLength) {
      throw new IOException("ReplicaFileLength:" + replicaFileLength
          + " or ecFileLength:" + ecFileLength
          + " is not same with " + fileLength);
    }
    LOG.info("ReplicaFileLength is {} and ecFileLength is {} and fileLength is {}.",
        replicaFileLength, ecFileLength, fileLength);
    FileChecksum replicaFullCK2 = replicaFS.getFileChecksum(replicateFilePath);
    FileChecksum ecFullCK2 = ecFS.getFileChecksum(ecFilePath);
    if (!replicaFullCK2.equals(ecFullCK2) || !replicaFullCK.equals(replicaFullCK2)) {
      throw new IOException("EcFullCK not equal with ReplicaFullCK");
    } else {
      LOG.info("ReplicaFullCK2 {} is same with ECFullCK2 {}.",
          replicaFullCK2, ecFullCK2);
    }

    for (int index = 0; index < loopNumber; index++) {
      long verifyLength = ThreadLocalRandom.current()
          .nextLong(1024, fileLength);
      verifyFileCheckSum(replicaFS, replicateFilePath,
          ecFS, ecFilePath, verifyLength);
    }
  }

  private void verifyFileCheckSum(FileSystem replicaFS, Path replicaPath,
      FileSystem ecFS, Path ecPath, long verifyLength) throws IOException {
    LOG.info("Will verify composite checksum between {} and {} with length {}.",
        replicaPath, ecPath, verifyLength);
    FileChecksum replicaCK = replicaFS.getFileChecksum(replicaPath, verifyLength);
    LOG.info("ReplicaCheckSum is {} and will get checksum from ecFile {}.",
        replicaCK, ecPath);
    FileChecksum ecCK = ecFS.getFileChecksum(ecPath, verifyLength);
    LOG.info("EC Checksum is {}.", ecCK);
    if (!replicaCK.equals(ecCK)) {
      throw new IOException("ReplicaCK:" + replicaCK
          + " is not same with ECCK:" + ecCK
          + " when the verifyLength is " + verifyLength);
    }
  }

  public void run(String[] args) throws IOException, NoSuchAlgorithmException {
    String cmd = args[0];
    switch (cmd) {
    case "testECFileWithPolicy": {
      Path testPath = new Path(args[1]);
      String ecPolicyName = args[2];
      long fileSize = Long.parseLong(args[3]);
      boolean useRandom = Boolean.parseBoolean(args[4]);
      testReadECFileWithCreate(
          testPath, ecPolicyName, fileSize, useRandom);
      break;
    }
    case "testReadExistFile": {
      Path testPath = new Path(args[1]);
      String expectedMD5 = args[2];
      testReadExistedECFile(testPath, expectedMD5);
      break;
    }
    case "testECPolicyInfo": {
      Path testPath = new Path(args[1]);
      testECPolicyInfo(testPath);
      break;
    }
    case "testWriteCornerCase": {
      Path testPath = new Path(args[1]);
      testWriteECFileWithCornerCase(testPath);
      break;
    }
    case "testReadWithCornerCase": {
      Path testPath = new Path(args[1]);
      testReadECFileWithCornerCase(testPath);
      break;
    }
    case "testReadRandomly": {
      Path existedPath = new Path(args[1]);
      Path testPath = new Path(args[2]);
      String ecPolicyName = args[3];
      int expectedLoopNum = Integer.parseInt(args[4]);
      testReadWithRandomOffsetAndLength(existedPath,
          testPath, ecPolicyName, expectedLoopNum);
      break;
    }
    case "testCompositeCRC": {
      Path replicateFilePath = new Path(args[1]);
      Path ecFilePath = new Path(args[2]);
      boolean useCache = Boolean.parseBoolean(args[3]);
      int loopNumber = Integer.parseInt(args[4]);
      testCompositeCrc(replicateFilePath, ecFilePath,
          useCache, loopNumber);
      break;
    }
    default:
      LOG.info("invalidate cmd " + cmd);
    }
    LOG.info("{} success, and args is {}", cmd, Arrays.asList(args));
  }

  public static void main(String[] args) throws Exception {
    DFSECStreamTest dfsecStreamTest = new DFSECStreamTest();
    dfsecStreamTest.run(args);
  }
}