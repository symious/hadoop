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

import com.google.protobuf.ByteString;
import org.apache.hadoop.hdfs.client.impl.BlockReaderFactory;
import org.apache.hadoop.hdfs.client.impl.BlockReaderRemote2;
import org.apache.hadoop.hdfs.protocol.BlockChecksumOptions;
import org.apache.hadoop.hdfs.protocol.BlockChecksumType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.Op;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpBlockChecksumResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.CrcUtil;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;

import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.SUCCESS;

/**
 * Utility classes to compute checksum for replicated block.
 */
public class BlockChecksumHelper {
  private static final Logger LOG = LoggerFactory.getLogger(
      BlockChecksumHelper.class);

  private final String src;
  private final DFSClient dfsClient;
  private final LocatedBlock locatedBlock;
  private final DatanodeInfo datanode;

  public BlockChecksumHelper(String src, DFSClient dfsClient,
      LocatedBlock locatedBlock, DatanodeInfo datanode) {
    this.src = src;
    this.dfsClient = dfsClient;
    this.locatedBlock = locatedBlock;
    this.datanode = datanode;
  }

  public BlockOpResponseProto compute(BlockChecksumType checksumType,
      long requestLength) throws IOException {
    if (checksumType.equals(BlockChecksumType.MD5CRC)) {
       return getMD5CRCFromDN(requestLength);
    } else if (checksumType.equals(BlockChecksumType.COMPOSITE_CRC)) {
      return getCompositeCRC(requestLength);
    }
    throw new IOException("Invalidate checksumType " + checksumType);
  }

  /**
   * Create and return a sender given an IO stream pair.
   */
  private Sender createSender(IOStreamPair pair) {
    DataOutputStream out = (DataOutputStream) pair.out;
    return new Sender(out);
  }

  /**
   * Get the MR5 checksum of the block directly from the DN.
   */
  private BlockOpResponseProto getMD5CRCFromDN(long requestLength)
      throws IOException {
    ExtendedBlock block = this.locatedBlock.getBlock();
    int tmpTimeout = 3000 + this.dfsClient.getConf().getSocketTimeout();
    try (IOStreamPair pair = this.dfsClient.connectToDN(this.datanode,
        tmpTimeout, this.locatedBlock.getBlockToken())) {

      LOG.debug("write to {}: {}, block={}", this.datanode,
          Op.BLOCK_CHECKSUM, block);
      block.setNumBytes(requestLength);

      // get block checksum
      createSender(pair).blockChecksum(block,
          locatedBlock.getBlockToken(),
          new BlockChecksumOptions(BlockChecksumType.MD5CRC));

      return DataTransferProtos.BlockOpResponseProto.parseFrom(
          PBHelperClient.vintPrefixed(pair.in));
    }
  }

  /**
   * Get the Composite checksum of the block by recomputing.
   */
  private BlockOpResponseProto getCompositeCRC(long requestLength)
      throws IOException {
    BlockChecksumOptions blockChecksumOptions =
        new BlockChecksumOptions(BlockChecksumType.COMPOSITE_CRC);
    BlockCompositeChecksumComputer maker =
        new BlockCompositeChecksumComputer(
            this.src, this.dfsClient, this.locatedBlock,
            this.datanode, requestLength);
    maker.compute();

    return BlockOpResponseProto.newBuilder()
        .setStatus(SUCCESS)
        .setChecksumResponse(OpBlockChecksumResponseProto.newBuilder()
            .setBytesPerCrc(maker.getBytesPerCRC())
            .setCrcPerBlock(maker.getCrcPerBlock())
            .setBlockChecksum(ByteString.copyFrom(maker.getOutBytes()))
            .setCrcType(PBHelperClient.convert(maker.getCrcType()))
            .setBlockChecksumOptions(
                PBHelperClient.convert(blockChecksumOptions)))
        .build();
  }

  /**
   * The abstract base block checksum computer, mainly for replicated blocks.
   */
  static class BlockCompositeChecksumComputer {
    private byte[] outBytes;
    private final ExtendedBlock block;
    // client side now can specify a range of the block for checksum
    private final long requestLength;
    private final boolean partialBlk;

    private DataChecksum checksum;
    private final String src;
    private final DFSClient dfsClient;
    private final LocatedBlock locatedBlock;
    private final DatanodeInfo datanodeInfo;

    BlockCompositeChecksumComputer(String src, DFSClient dfsClient,
        LocatedBlock locatedBlock, DatanodeInfo datanodeInfo,
        long requestLength) {
      this.src = src;
      this.dfsClient = dfsClient;
      this.locatedBlock = locatedBlock;
      this.datanodeInfo = datanodeInfo;
      this.block = this.locatedBlock.getBlock();
      this.requestLength = requestLength;
      Preconditions.checkArgument(requestLength >= 0);
      this.partialBlk = requestLength < block.getNumBytes();
    }

    /**
     * Get the remote block reader for the block.
     */
    private BlockReaderRemote2 getBlockReader(String src,
        DFSClient dfsClient, DatanodeInfo datanodeInfo,
        LocatedBlock targetBlock, long length)
        throws IOException {
      ExtendedBlock blk = targetBlock.getBlock();
      Token<BlockTokenIdentifier> accessToken = targetBlock.getBlockToken();
      boolean verifyChecksum = true;
      final String dnAddr = datanodeInfo.getXferAddr(
          dfsClient.getConf().isConnectToDnViaHostname());
      DFSClient.LOG.debug("Connecting to datanode {}", dnAddr);
      InetSocketAddress targetAddr = NetUtils.createSocketAddr(dnAddr);
      return new BlockReaderFactory(dfsClient.getConf()).
          setInetSocketAddress(targetAddr).
          setRemotePeerFactory(dfsClient).
          setDatanodeInfo(datanodeInfo).
          setFileName(src).
          setBlock(blk).
          setBlockToken(accessToken).
          setStartOffset(0).
          setVerifyChecksum(verifyChecksum).
          setClientName(dfsClient.clientName).
          setLength(length).
          setClientCacheContext(dfsClient.getClientContext()).
          setCachingStrategy(dfsClient.getDefaultReadCachingStrategy()).
          setUserGroupInformation(dfsClient.ugi).
          setConfiguration(dfsClient.getConfiguration()).
          setTracer(dfsClient.getTracer())
          .getRemote2BlockReaderFromTcp();
    }

    void compute() throws IOException {
      long computeLength = Math.min(this.requestLength,
          this.block.getNumBytes());
      HdfsCrcComposer cachedComposer = getComposerFromCache(
          computeLength, this.block);
      if (cachedComposer != null) {
        this.checksum = cachedComposer.getChecksum();
        setOutCompositeCrc(cachedComposer.digest());
      } else {
        computeCrcOnline(computeLength);
      }
    }

    private void setOutCompositeCrc(byte[] composedCrcs)
        throws IOException {
      setOutBytes(composedCrcs);
      if (LOG.isDebugEnabled()) {
        LOG.debug("block={}, getBytesPerCRC={}, crcPerBlock={}," +
                " compositeCrc={}, requestLength={}",
            this.block, getBytesPerCRC(), getCrcPerBlock(),
            CrcUtil.toMultiCrcString(composedCrcs), this.requestLength);
      }
    }

    /**
     * Try to get one cached HdfsCrcComposer.
     */
    private HdfsCrcComposer getComposerFromCache(
        long expectedLength, ExtendedBlock extendedBlock) {
      HdfsCrcComposer hdfsCrcComposer = this.dfsClient.getClientContext()
          .getBlockCompositeCrcCache().getCrcComposer(extendedBlock);
      if (hdfsCrcComposer != null && hdfsCrcComposer.isClosed()
          && hdfsCrcComposer.getLength() == expectedLength) {
        return hdfsCrcComposer;
      }
      return null;
    }

    private void computeCrcOnline(long readLength) throws IOException {
      BlockReaderRemote2 blockReader = null;
      try {
        blockReader = getBlockReader(this.src, this.dfsClient,
            this.datanodeInfo, this.locatedBlock, readLength);
        this.checksum = blockReader.getChecksum();
        computeCompositeCrc(blockReader, readLength);
      } finally {
        if (blockReader != null) {
          blockReader.close();
        }
      }
    }

    void setOutBytes(byte[] bytes) {
      this.outBytes = bytes;
    }

    byte[] getOutBytes() {
      return outBytes;
    }

    int getBytesPerCRC() {
      return this.checksum.getBytesPerChecksum();
    }

    DataChecksum.Type getCrcType() {
      return this.checksum.getChecksumType();
    }

    int getChecksumSize() {
      return this.checksum.getChecksumSize();
    }

    boolean isPartialBlk() {
      return partialBlk;
    }

    long getCrcPerBlock() {
      return (this.block.getNumBytes() - 1) / getBytesPerCRC() + 1;
    }

    private void computeCompositeCrc(BlockReaderRemote2 blockReader,
        long computeLength) throws IOException {
      HdfsCrcComposer hdfsCrcComposer =
          HdfsCrcComposer.newStripedCrcComposer(
              this.checksum, getBytesPerCRC(), computeLength);

      // Whether getting the checksum for the entire block (which itself may
      // not be a full block size and may have a final chunk smaller than
      // getBytesPerCRC()), we begin with a number of full chunks, all size
      // getBytesPerCRC().
      long bytesPerCrc = getBytesPerCRC();
      long numFullChunks = computeLength / bytesPerCrc;
      for (long i = 0; i < numFullChunks; i++) {
        int crcB = blockReader.getOneCRC();
        hdfsCrcComposer.update(crcB, bytesPerCrc);
      }

      // There may be a final partial chunk that is not full-sized. Unlike the
      // MD5 case, we still consider this a "partial chunk" even if
      // getRequestLength() == getVisibleLength(), since the CRC composition
      // depends on the byte size of that final chunk, even if it already has a
      // precomputed CRC stored in metadata. So there are two cases:
      //   1. Reading only part of a block via getRequestLength(); we get the
      //      crcPartialBlock() explicitly.
      //   2. Reading full visible length; the partial chunk already has a CRC
      //      stored in block metadata, so we just continue reading checksumIn.
      long partialChunkSize = computeLength % bytesPerCrc;
      if (partialChunkSize > 0) {
        if (isPartialBlk()) {
          byte[] partialChunkCrcBytes = crcPartialBlock(computeLength, blockReader);
          hdfsCrcComposer.update(partialChunkCrcBytes, 0,
              partialChunkCrcBytes.length, partialChunkSize);
        } else {
          int partialChunkCrc = blockReader.getOneCRC();
          hdfsCrcComposer.update(partialChunkCrc, partialChunkSize);
        }
      }
      hdfsCrcComposer.closeUpdate();
      setOutCompositeCrc(hdfsCrcComposer.digest());
    }

    /**
     * Calculate partial block checksum.
     */
    byte[] crcPartialBlock(long computeLength, BlockReaderRemote2 blockReader)
        throws IOException {
      int partialLength = (int) (computeLength % getBytesPerCRC());
      if (partialLength > 0) {
        byte[] buf = new byte[partialLength];
        // long offset = this.requestLength - partialLength;
        // TODO verify the offset of the blockReader.
        // Get the CRC of the partialLength.
        blockReader.read(buf, 0, partialLength);
        this.checksum.reset();
        this.checksum.update(buf, 0, partialLength);
        byte[] partialCrc = new byte[getChecksumSize()];
        this.checksum.writeValue(partialCrc, 0, true);
        return partialCrc;
      }
      return null;
    }
  }
}
