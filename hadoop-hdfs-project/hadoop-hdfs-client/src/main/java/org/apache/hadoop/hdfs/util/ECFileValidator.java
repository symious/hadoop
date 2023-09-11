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

package org.apache.hadoop.hdfs.util;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.impl.BlockReaderRemote;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * EC File validator tool.
 */
@InterfaceAudience.Private
public class ECFileValidator implements Closeable {

  private final static Logger LOG = LoggerFactory.getLogger(ECFileValidator.class);

  private final Configuration conf;
  private final DFSClient client;
  private final boolean useDNHostname;
  private final CachingStrategy cachingStrategy;
  private final int stripedReadBufferSize;
  private ThreadPoolExecutor executor;
  private final DistributedFileSystem dfs;
  public final static String EC_FILE_FAIL_BLOCK = "fileFailed";

  public ECFileValidator(Configuration conf) throws IOException {
    this(FileSystem.get(conf));
  }

  public ECFileValidator(FileSystem fileSystem) {
    this.dfs = (DistributedFileSystem) fileSystem;
    this.conf = fileSystem.getConf();
    this.client = dfs.getClient();
    this.useDNHostname = this.conf.getBoolean(HdfsClientConfigKeys.DFS_DATANODE_USE_DN_HOSTNAME,
        HdfsClientConfigKeys.DFS_DATANODE_USE_DN_HOSTNAME_DEFAULT);
    this.cachingStrategy = CachingStrategy.newDefaultStrategy();
    this.stripedReadBufferSize = conf.getInt(
        HdfsClientConfigKeys.DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_BUFFER_SIZE_KEY,
        HdfsClientConfigKeys.DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_BUFFER_SIZE_DEFAULT);
    int threads = conf.getInt(HdfsClientConfigKeys.DFS_EC_VALIDATOR_THREADS_KEY,
        HdfsClientConfigKeys.DFS_EC_VALIDATOR_THREADS_DEFAULT);
    LOG.info("Create striped reader service pool with {} threads", threads);
    this.executor = DFSUtilClient.getThreadPoolExecutor(threads,
        threads, 60, "StripedRead-", true);
    this.executor.allowCoreThreadTimeOut(true);
  }

  public List<ECBlockValidatorReport> verifyECFile(String file, boolean ignoreFailures)
      throws IOException {
    return verifyECFile(file, "", ignoreFailures);
  }

  /**
   *
   * @param file the hdfs file of erasure coded.
   * @param blockGroupName the block file name for the block, such as blk_xxx.
   * @param ignoreFailures if false, verify exception block group exit,
   *                       if true, continue to verify all block groups of file.
   * @return
   * @throws IOException
   */
  public List<ECBlockValidatorReport> verifyECFile(String file, String blockGroupName,
      boolean ignoreFailures)
      throws IOException {
    List<ECBlockValidatorReport> blockValidatorReports = Lists.newArrayList();
    Path path = new Path(file);
    FileStatus fileStatus;
    try {
      fileStatus = dfs.getFileStatus(path);
    } catch (FileNotFoundException e) {
      blockValidatorReports.add(createFailedReport("File " + file + " does not exist."));
      return blockValidatorReports;
    }

    if (!fileStatus.isFile()) {
      blockValidatorReports.add(createFailedReport("File " + file + " is not a regular file."));
      return blockValidatorReports;
    }

    if (!dfs.isFileClosed(path)) {
      blockValidatorReports.add(createFailedReport("File " + file + " is not closed."));
      return blockValidatorReports;
    }

    LocatedBlocks locatedBlocks = client.getLocatedBlocks(file, 0, fileStatus.getLen());
    if (locatedBlocks.getErasureCodingPolicy() == null) {
      blockValidatorReports.add(createFailedReport("File " + file + " is not erasure coded."));
      return blockValidatorReports;
    }

    if (locatedBlocks.locatedBlockCount() == 0) {
      blockValidatorReports.add(createFailedReport("File " + file + " size is zero."));
      return blockValidatorReports;
    }

    ErasureCodingPolicy ecPolicy = locatedBlocks.getErasureCodingPolicy();
    int dataBlkNum = ecPolicy.getNumDataUnits();
    int parityBlkNum = ecPolicy.getNumParityUnits();
    int cellSize = ecPolicy.getCellSize();
    RawErasureEncoder encoder = CodecUtil.createRawEncoder(this.conf, ecPolicy.getCodecName(),
        new ErasureCoderOptions(dataBlkNum, parityBlkNum));
    int blockNum = dataBlkNum + parityBlkNum;
    BlockReader[] blockReaders = new BlockReader[blockNum];

    try {
      for (LocatedBlock locatedBlock : locatedBlocks.getLocatedBlocks()) {
        LocatedStripedBlock blockGroup = (LocatedStripedBlock) locatedBlock;
        if (StringUtils.isNullOrEmpty(blockGroupName) ||
            blockGroup.getBlock().getBlockName().equals(blockGroupName)) {
          ECBlockValidatorReport ecBlockValidatorReport = null;
          try {
            ecBlockValidatorReport = verifyBlockGroup(blockGroup, file,
                cellSize, dataBlkNum, parityBlkNum, blockReaders, encoder);
          } catch (Exception e) {
            ecBlockValidatorReport = new ECBlockValidatorReport(blockGroup.getBlock().
                getBlockName());
            ecBlockValidatorReport.addFailedBlockReports(e.getMessage());
          } finally {
            closeBlockReaders(blockReaders);
          }
          blockValidatorReports.add(ecBlockValidatorReport);

          if (!ignoreFailures && !ecBlockValidatorReport.isHealthy()) {
            LOG.info("Block group {} is first failures block, as ignoreFailures is false and exit.",
                blockGroup.getBlock().getBlockName());
            break;
          }
        }
      }
    } finally {
      if (encoder != null) {
        encoder.release();
      }
    }
    return blockValidatorReports;
  }

  private ECBlockValidatorReport verifyBlockGroup(LocatedStripedBlock blockGroup, String file,
      int cellSize, int dataBlkNum, int parityBlkNum, BlockReader[] blockReaders,
      RawErasureEncoder encoder) throws Exception {
    final ECBlockValidatorReport ecBlockValidatorReport = new ECBlockValidatorReport(blockGroup.
        getBlock().getBlockName());
    final LocatedBlock[] indexedBlocks = StripedBlockUtil.parseStripedBlockGroup(blockGroup,
        cellSize, dataBlkNum, parityBlkNum);

    int blockNumExpected = Math.min(dataBlkNum,
        (int) ((blockGroup.getBlockSize() - 1) / cellSize + 1)) + parityBlkNum;
    if (blockGroup.getBlockIndices().length < blockNumExpected) {
      LOG.warn("Block group {} is under-erasure-coded.", blockGroup.getBlock().toString());
      ecBlockValidatorReport.setUnder(true);
      return ecBlockValidatorReport;
    }

    Validator validator = null;
    try {
      validator = new Validator(blockGroup, file, dataBlkNum, parityBlkNum, encoder);

      validator.initReaders(indexedBlocks, blockReaders);

      validator.initBufferSize();

      validator.validate(indexedBlocks, blockReaders, ecBlockValidatorReport);
    } finally {
      clearBuffers(validator);
    }

    return ecBlockValidatorReport;
  }

  private void clearBuffers(Validator validator) {
    if (validator != null) {
      if (validator.buffers != null) {
        for (ByteBuffer buffer : validator.buffers) {
          buffer.clear();
        }
      }
      if (validator.outputs != null) {
        for (ByteBuffer buffer : validator.outputs) {
          buffer.clear();
        }
      }
    }
  }

  private void closeBlockReaders(BlockReader[] blockReaders) {
    for (int i = 0; i < blockReaders.length; i++) {
      if (blockReaders[i] != null) {
        IOUtils.closeStream(blockReaders[i]);
        blockReaders[i] = null;
      }
    }
  }

  public ECBlockValidatorReport createFailedReport(String message) {
    return new ECBlockValidatorReport(EC_FILE_FAIL_BLOCK, message);
  }

  @Override
  public void close() throws IOException {
    if (executor != null) {
      executor.shutdown();
      executor = null;
    }
  }

  class Validator {
    private final LocatedStripedBlock blockGroup;
    private final String file;
    private final int dataBlkNum;
    private final int parityBlkNum;
    private final RawErasureEncoder encoder;
    private long maxBlockLen = 0L;
    private DataChecksum checksum = null;
    private ByteBuffer[] buffers;
    private ByteBuffer[] outputs;
    private int bufferSize;

    public Validator(LocatedStripedBlock blockGroup, String file, int dataBlkNum, int parityBlkNum,
        RawErasureEncoder encoder) {
      this.blockGroup = blockGroup;
      this.file = file;
      this.dataBlkNum = dataBlkNum;
      this.parityBlkNum = parityBlkNum;
      this.encoder = encoder;
    }

    private void initReaders(LocatedBlock[] indexedBlocks, BlockReader[] blockReaders)
        throws IOException {
      for (int i = 0; i < dataBlkNum + parityBlkNum; i++) {
        LocatedBlock block = indexedBlocks[i];
        if (block == null) {
          blockReaders[i] = null;
          continue;
        }
        if (block.getBlockSize() > maxBlockLen) {
          maxBlockLen = block.getBlockSize();
        }
        BlockReader blockReader = createBlockReader(block.getBlock(), block.getLocations()[0],
            block.getBlockToken(), file);
        DataChecksum dataChecksum = blockReader.getDataChecksum();
        if (checksum == null) {
          checksum = dataChecksum;
        } else {
          if (!checksum.equals(dataChecksum)) {
            throw new IOException("Checksum not matched: expect checksum=" + checksum
                + " but actual checksum=" + dataChecksum + " from "
                + block.getBlock().getBlockName());
          }
        }
        blockReaders[i] = blockReader;
      }
    }

    private BlockReader createBlockReader(ExtendedBlock block, DatanodeInfo dnInfo,
        Token<BlockTokenIdentifier> token, String file) throws IOException {
      InetSocketAddress dnAddress = NetUtils.createSocketAddr(dnInfo.getXferAddr(useDNHostname));
      Peer peer = client.newConnectedPeer(dnAddress, token, dnInfo);
      return BlockReaderRemote.newBlockReader(
          file, block, token, 0,
          block.getNumBytes(), true, "", peer, dnInfo,
          null, cachingStrategy, -1, conf);
    }

    private void initBufferSize() {
      assert checksum != null;
      int bytesPerChecksum = checksum.getBytesPerChecksum();
      bufferSize = stripedReadBufferSize < bytesPerChecksum ? bytesPerChecksum :
          stripedReadBufferSize - stripedReadBufferSize % bytesPerChecksum;
      buffers = new ByteBuffer[dataBlkNum + parityBlkNum];
      outputs = new ByteBuffer[parityBlkNum];
      for (int i = 0; i < dataBlkNum + parityBlkNum; i++) {
        buffers[i] = ByteBuffer.allocate(bufferSize);
      }
      for (int i = 0; i < parityBlkNum; i++) {
        outputs[i] = ByteBuffer.allocate(bufferSize);
      }
    }

    private void validate(LocatedBlock[] indexedBlocks, BlockReader[] blockReaders,
        ECBlockValidatorReport ecBlockValidatorReport) throws IOException {
      long positionInBlock = 0L;
      while (positionInBlock < maxBlockLen) {
        final int toVerifyLen = (int) Math.min(bufferSize, maxBlockLen - positionInBlock);

        // step1: read block data from dn.
        readStripedBlock(toVerifyLen, indexedBlocks, blockReaders, ecBlockValidatorReport);
        if (ecBlockValidatorReport.isFailed() || ecBlockValidatorReport.isCheckSumFailed()) {
          return;
        }

        // step2: encode with inputs and generates outputs.
        encode(toVerifyLen);

        // step3: compare outputs and original parity.
        compare(indexedBlocks, ecBlockValidatorReport);
        if (ecBlockValidatorReport.isCorrupt()) {
          return;
        }

        positionInBlock += toVerifyLen;
      }
      ecBlockValidatorReport.setHealthy(true);
    }

    private void readStripedBlock(int toVerifyLen, LocatedBlock[] indexedBlocks,
        BlockReader[] blockReaders, ECBlockValidatorReport ecBlockValidatorReport) {
      List<Future<Integer>> futures = new ArrayList<>(dataBlkNum + parityBlkNum);
      try {
        for (int i = 0; i < dataBlkNum + parityBlkNum; i++) {
          futures.add(submitBlockReadTask(blockReaders[i], buffers[i], toVerifyLen));
        }

        for (int i = 0; i < dataBlkNum + parityBlkNum; i++) {
          try {
            futures.get(i).get();
          } catch (Exception e) {
            handleBlockReadException(e, indexedBlocks[i], file, ecBlockValidatorReport);
          }
        }
      } finally {
        cancelAndClearFutures(futures);
      }
    }

    private void cancelAndClearFutures(List<Future<Integer>> futures) {
      for (Future<Integer> future : futures) {
        future.cancel(true);
      }
      futures.clear();
    }

    private Future<Integer> submitBlockReadTask(BlockReader blockReader, ByteBuffer buffer,
        int toVerifyLen) {
      return executor.submit(() -> {
        buffer.clear();
        buffer.limit(toVerifyLen);
        int readLen = 0;
        if (blockReader != null) {
          int toRead = buffer.remaining();
          while (readLen < toRead) {
            int nread = blockReader.read(buffer);
            if (nread <= 0) {
              break;
            }
            readLen += nread;
          }
        }
        while (buffer.hasRemaining()) {
          buffer.put((byte) 0);
        }
        buffer.flip();
        return readLen;
      });
    }

    private void handleBlockReadException(Exception e, LocatedBlock indexedBlock, String file,
        ECBlockValidatorReport ecBlockValidatorReport) {
      String internalBlock = indexedBlock.getBlock().getLocalBlock().toString();
      String datanodeInfo = indexedBlock.getLocations()[0].getXferAddr();
      if ((e.getCause() instanceof ChecksumException)) {
        LOG.warn("Block group {} read {} found Checksum error for {} from {} cause: {}",
            blockGroup.getBlock().toString(), internalBlock, file, datanodeInfo,
            e.getMessage());
        ecBlockValidatorReport.addCheckSumFailedBlockReports(internalBlock, datanodeInfo,
            e.getMessage());
      } else {
        LOG.warn("Block group {} read {} found Unknown error for {} from {} cause: {}",
            blockGroup.getBlock().toString(), internalBlock, file, datanodeInfo,
            e.getMessage());
        ecBlockValidatorReport.addFailedBlockReports(internalBlock, datanodeInfo,
            e.getMessage());
      }
    }

    private void encode(int toVerifyLen) throws IOException {
      ByteBuffer[] inputs = new ByteBuffer[dataBlkNum];
      System.arraycopy(buffers, 0, inputs, 0, dataBlkNum);
      for (int i = 0; i < parityBlkNum; i++) {
        outputs[i].clear();
        outputs[i].limit(toVerifyLen);
      }
      encoder.encode(inputs, outputs);
    }

    private void compare(LocatedBlock[] indexedBlocks,
        ECBlockValidatorReport ecBlockValidatorReport) {
      for (int i = 0; i < parityBlkNum; i++) {
        if (!buffers[dataBlkNum + i].equals(outputs[i])) {
          String internalBlock = indexedBlocks[dataBlkNum + i].getBlock().getLocalBlock().toString();
          String datanodeInfo = indexedBlocks[dataBlkNum + i].getLocations()[0].getXferAddr();
          LOG.warn("Block group {} for {} from {} compute result not match.",
              blockGroup.getBlock().toString(), internalBlock, datanodeInfo);
          ecBlockValidatorReport.addCorruptBlockReports(internalBlock, datanodeInfo);
        }
      }
    }
  }
}
