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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_BLOCK_SIZE_KEY;

/**
 * A class to verify EC process with DistcpJob.
 */
public class DistCpTestJobForEC {
  private static final Logger LOG =
      LoggerFactory.getLogger(DistCpTestJobForEC.class);
  private final String NORMAL_DIR = "replicaDir";
  private final String EC_DIR_1 = "stripedDir1";
  private final String EC_DIR_2 = "stripedDir2";
  private final String EC_DIR_3 = "stripedDir3";
  private final int FILE_MIN_LENGTH = 100 * 1024 * 1024;
  private final int FILE_MAX_LENGTH = 150 * 1024 * 1024;
  private final int DEFAULT_BATCH_SIZE = 10 * 1024 * 1024;

  private final Configuration conf;
  private final Path baseDir;
  private final String ecPolicyName1;
  private final String ecPolicyName2;
  private final int fileNumber;
  private final int maxMap;
  private final int bandwidthMB;
  private final boolean createNewDataEachLoop;
  private final int expectedLoopNum;
  private final Map<String, FileChecksum> fileCheckSums;

  public DistCpTestJobForEC(Path baseDir, String ecPolicyName1,
      String ecPolicyName2, int maxMap, int bandwidthMB,
      boolean createNewDataEachLoop, int expectedLoopCounter,
      int fileNumber) {
    this.conf = new HdfsConfiguration();
    this.baseDir = baseDir;
    this.ecPolicyName1 = ecPolicyName1;
    this.ecPolicyName2 = ecPolicyName2;
    this.maxMap = maxMap;
    this.bandwidthMB = bandwidthMB;
    this.createNewDataEachLoop = createNewDataEachLoop;
    this.expectedLoopNum = expectedLoopCounter;
    this.fileCheckSums = new HashMap<>();
    this.fileNumber = fileNumber;
  }

  public void doVerify() throws Exception {
    int loopCounter = 0;
    while (loopCounter++ < this.expectedLoopNum) {
      LOG.info("LoopNumber_{} begin", loopCounter);

      // Step1: Create new data
      Path normalDir = new Path(this.baseDir, NORMAL_DIR);
      if (createNewDataEachLoop || fileCheckSums.isEmpty()) {
        generateNewData(normalDir);
      }

      LOG.info("LoopNumber_{} step1 completed, and normalDir is {}.", loopCounter, normalDir);

      // Step2: Distcp the data to EC dir1
      Path stripeDir1 = new Path(this.baseDir, EC_DIR_1);
      mkdirWithECPolicy(stripeDir1, this.ecPolicyName1);
      createOneDistcpJob(normalDir, stripeDir1, false);
      verifyFileECPolicy(stripeDir1, true, ecPolicyName1);
      deleteDir(normalDir);
      LOG.info("LoopNumber_{} step2 completed, and stripeDir1 is {}," +
          " ecPolicyName1 is {}.", loopCounter, stripeDir1, ecPolicyName1);

      // Step3: Distcp the data to EC dir2
      Path stripeDir2 = new Path(this.baseDir, EC_DIR_2);
      mkdirWithECPolicy(stripeDir2, null);
      createOneDistcpJob(stripeDir1, stripeDir2, true);
      verifyFileECPolicy(stripeDir2, true, ecPolicyName1);
      deleteDir(stripeDir1);
      LOG.info("LoopNumber_{} step3 completed, and stripeDir2 is {}," +
          " ecPolicyName1 is {}.", loopCounter, stripeDir2, ecPolicyName1);

      // Step4: Distcp the data to EC dir3
      Path stripeDir3 = new Path(this.baseDir, EC_DIR_3);
      mkdirWithECPolicy(stripeDir3, this.ecPolicyName2);
      createOneDistcpJob(stripeDir2, stripeDir3, false);
      verifyFileECPolicy(stripeDir3, true, ecPolicyName2);
      deleteDir(stripeDir2);
      LOG.info("LoopNumber_{} step4 completed, and stripeDir3 is {}," +
          " ecPolicyName2 is {}.", loopCounter, stripeDir3, ecPolicyName2);

      // Step5: Distcp the data to Normal dir1
      mkdirWithECPolicy(normalDir, null);
      createOneDistcpJob(stripeDir3, normalDir, false);
      verifyFileECPolicy(normalDir, false, null);
      deleteDir(stripeDir3);
      LOG.info("LoopNumber_{} step5 completed, and normalDir is {}",
          loopCounter, normalDir);

      // Step5: Verify check
      verifyFileChecksum(normalDir);
      if (createNewDataEachLoop) {
        deleteDir(normalDir);
        this.fileCheckSums.clear();
      }

      LOG.info("LoopNumber_{} completed", loopCounter);
    }
  }

  /**
   * Verify the file checksum.
   */
  private void verifyFileChecksum(Path dir)
      throws IOException {
    DistributedFileSystem dfs = (DistributedFileSystem)
        dir.getFileSystem(this.conf);
    FileStatus[] fileStatus = dfs.listStatus(dir);
    for (FileStatus status : fileStatus) {
      FileChecksum fileChecksum = dfs.getFileChecksum(status.getPath());
      FileChecksum storeChecksum = this.fileCheckSums.get(
          status.getPath().toUri().getPath());
      if (!fileChecksum.equals(storeChecksum)) {
        throw new IOException("FileChecksum " + fileChecksum
            + " not equal with " + storeChecksum
            + " for " + status.getPath());
      }
      LOG.info("FileChecksum of {} verify success.", status.getPath());
    }
  }

  /**
   * Delete the dir.
   */
  private void deleteDir(Path dir) throws IOException {
    if (!this.conf.getBoolean("mock.delete", true)) {
      FileSystem fs = dir.getFileSystem(this.conf);
      fs.delete(dir, true);
    }
    LOG.info("Deleted {}", dir);
  }

  /**
   * Verify ECPolicy info.
   */
  private void verifyFileECPolicy(Path dir, boolean ecFile, String ecPolicyName)
      throws IOException {
    DistributedFileSystem dfs = (DistributedFileSystem)
        dir.getFileSystem(this.conf);
    FileStatus[] fileStatus = dfs.listStatus(dir);
    for (FileStatus status : fileStatus) {
      ErasureCodingPolicy fileECPolicy =
          dfs.getErasureCodingPolicy(status.getPath());
      if (!ecFile && fileECPolicy != null && fileECPolicy.getName() != null) {
        throw new IOException("Found ecPolicy " + fileECPolicy
            + " for " + status.getPath());
      }

      if (ecFile && (fileECPolicy == null ||
          !ecPolicyName.equals(fileECPolicy.getName()))) {
        throw new IOException("Found invalidate ecPolicy Name "
            + fileECPolicy + " for " + status.getPath()
            + ", and expected is " + ecPolicyName);
      }
    }
  }

  /**
   * Create one distcp job and execute it.
   */
  private void createOneDistcpJob(Path sourceDir, Path destDir, boolean keepEC)
      throws Exception {
    DistCpOptions inputOptions = new DistCpOptions(
        Collections.singletonList(sourceDir), destDir);

    //inputOptions.preserve(DistCpOptions.FileAttribute.REPLICATION);
    inputOptions.preserve(DistCpOptions.FileAttribute.BLOCKSIZE);
    inputOptions.preserve(DistCpOptions.FileAttribute.USER);
    inputOptions.preserve(DistCpOptions.FileAttribute.GROUP);
    inputOptions.preserve(DistCpOptions.FileAttribute.PERMISSION);
    inputOptions.preserve(DistCpOptions.FileAttribute.CHECKSUMTYPE);
    inputOptions.preserve(DistCpOptions.FileAttribute.ACL);
    //inputOptions.preserve(DistCpOptions.FileAttribute.XATTR);
    inputOptions.preserve(DistCpOptions.FileAttribute.TIMES);
    if (keepEC) {
      inputOptions.preserve(DistCpOptions.FileAttribute.ERASURECODINGPOLICY);
    }

    if (conf.getBoolean("skip.sync", true)) {
      inputOptions.setSyncFolder(true);
      inputOptions.setSkipCRC(true);
    }
    inputOptions.setMaxMaps(this.maxMap);
    inputOptions.setMapBandwidth(this.bandwidthMB);

    DistCp distCp = new DistCp(this.conf, inputOptions);
    Job job = distCp.execute();
    LOG.info("Distcp job complete from {} to {}, and job status is {}.",
        sourceDir, destDir, job);
  }

  /**
   * Generate some files and store checksums.
   */
  private void generateNewData(Path normalPath) throws IOException {
    mkdirWithECPolicy(normalPath, null);
    for (int fileIndex = 0; fileIndex < this.fileNumber; fileIndex++) {
      Path filePath = new Path(normalPath, String.valueOf(fileIndex));
      long fileLength = ThreadLocalRandom.current().
          nextLong(FILE_MIN_LENGTH, FILE_MAX_LENGTH);
      crateFileAndStoreCheckSum(filePath, fileLength);
    }

    generateCornerData(normalPath, ecPolicyName1);
    generateCornerData(normalPath, ecPolicyName2);
  }

  private void crateFileAndStoreCheckSum(Path filePath, long fileLength)
      throws IOException {
    FileChecksum fileChecksum = generateFile(filePath, fileLength);
    this.fileCheckSums.put(filePath.toUri().getPath(), fileChecksum);
  }

  private void generateCornerData(Path normalPath, String ecPolicyName)
      throws IOException {
    ErasureCodingPolicy ecPolicy = SystemErasureCodingPolicies.getByName(ecPolicyName);
    if (ecPolicy == null) {
      throw new IOException("There is no valid ec policy for " + ecPolicyName);
    }
    int cellSize = ecPolicy.getCellSize();
    int dataUnits = ecPolicy.getNumDataUnits();
    int parityUnits = ecPolicy.getNumParityUnits();

    // corner case 1: file size less than cellSize * dataUnits
    for (int index = 0; index < 10; index++) {
      Path filePath = new Path(normalPath, "corner_case_1_" + index);
      long fileLength = ThreadLocalRandom.current().nextLong(0,
          (long) dataUnits * cellSize);
      crateFileAndStoreCheckSum(filePath, fileLength);
    }

    long defaultBlockSize = this.conf.getLongBytes(
        DFS_BLOCK_SIZE_KEY, DFS_BLOCK_SIZE_DEFAULT);

    // corner case 2: file size more than cellSize * dataUnits but less than block size.
    for (int index = 0; index < 10; index++) {
      Path filePath = new Path(normalPath, "corner_case_2_" + index);
      long fileLength = ThreadLocalRandom.current().nextLong(
          (long) dataUnits * cellSize, defaultBlockSize);
      crateFileAndStoreCheckSum(filePath, fileLength);
    }

    // corner case 3: file size more than cellSize * (dataUnits + parityUnits) * blockSize
    for (int index = 0; index < 10; index++) {
      Path filePath = new Path(normalPath, "corner_case_3_" + index);
      long fileLength = ThreadLocalRandom.current().nextLong(
          (long) (dataUnits + parityUnits) * defaultBlockSize,
          (long) 2 * (dataUnits + parityUnits) * defaultBlockSize);
      crateFileAndStoreCheckSum(filePath, fileLength);
    }
  }

  /**
   * Generate some random data.
   */
  private byte[] generateRandomData(int len) {
    byte[] buffer = new byte[len];
    for (int i = 0; i < buffer.length; i++) {
      buffer[i] = (byte) ThreadLocalRandom.current().nextInt(256);
    }
    return buffer;
  }

  /**
   * Create one file and fill some data.
   * @return the file checksum.
   */
  private FileChecksum generateFile(Path filePath, long fileLength)
      throws IOException {
    FileSystem fs = filePath.getFileSystem(this.conf);
    FSDataOutputStream fsDataOutputStream = fs.create(filePath);

    int writeLength = 0;
    while (writeLength < fileLength) {
      long currentBatchSize = Math.min(fileLength - writeLength, DEFAULT_BATCH_SIZE);
      byte[] bytes = generateRandomData((int) currentBatchSize);
      fsDataOutputStream.write(bytes);
      writeLength += currentBatchSize;
    }
    fsDataOutputStream.close();

    return fs.getFileChecksum(filePath);
  }


  /**
   * Mkdir one none exited directory, and set the ecPolicyName
   * to it if ecPolicyName is valid.
   */
  private void mkdirWithECPolicy(Path dirPath, String ecPolicyName)
      throws IOException {
    DistributedFileSystem dfs = (DistributedFileSystem)
        dirPath.getFileSystem(this.conf);
    if (dfs.exists(dirPath)) {
      throw new IOException("The normalPath " + dirPath
          + " already existed, please check it.");
    }

    dfs.mkdirs(dirPath);
    if (ecPolicyName != null) {
      dfs.getClient().setErasureCodingPolicy(
          dirPath.toUri().getPath(), ecPolicyName);
    }
  }

  public static void main(String args[]) throws Exception {
    Path baseDir = new Path(args[0]);
    String ecPolicyName1 = args[1];
    String ecPolicyName2 = args[2];
    int maxMap = Integer.parseInt(args[3]);
    int bandwidthMB = Integer.parseInt(args[4]);
    boolean createNewDataEachLoop = Boolean.parseBoolean(args[5]);
    int expectedLoopNumber = Integer.parseInt(args[6]);
    int expectedFileNumber = Integer.parseInt(args[7]);

    DistCpTestJobForEC ecTest = new DistCpTestJobForEC(baseDir,
        ecPolicyName1, ecPolicyName2, maxMap, bandwidthMB,
        createNewDataEachLoop, expectedLoopNumber, expectedFileNumber);
    ecTest.doVerify();
  }
}