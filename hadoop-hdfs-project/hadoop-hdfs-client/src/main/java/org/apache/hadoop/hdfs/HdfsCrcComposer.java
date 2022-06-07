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

import org.apache.hadoop.util.CrcComposer;
import org.apache.hadoop.util.CrcUtil;
import org.apache.hadoop.util.DataChecksum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class HdfsCrcComposer extends CrcComposer {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsCrcComposer.class);

  private volatile long expectedNextOffset = 0;
  private boolean canUpdate = true;
  private final DataChecksum checksum;
  private final long maxLength;

  public HdfsCrcComposer(DataChecksum checksum,
      int crcPolynomial, int precomputedMonomialForHint,
      long bytesPerCrcHint, long stripeLength) {
    super(crcPolynomial, precomputedMonomialForHint,
        bytesPerCrcHint, stripeLength);
    this.checksum = checksum;
    this.maxLength = stripeLength;
    LOG.debug("Checksum is {}.", this.checksum);
  }

  public DataChecksum getChecksum() {
    return this.checksum;
  }

  private int getBytesPerCrc() {
    return this.checksum.getBytesPerChecksum();
  }

  public static HdfsCrcComposer newStripedCrcComposer(
      DataChecksum checksum, long bytesPerCrcHint,
      long stripeLength) throws IOException {
    int polynomial = DataChecksum.getCrcPolynomialForType(
        checksum.getChecksumType());
    return new HdfsCrcComposer(checksum, polynomial,
        CrcUtil.getMonomial(bytesPerCrcHint, polynomial),
        bytesPerCrcHint, stripeLength);
  }

  public synchronized void update(long offsetInBlock,
      long dataLength, ByteBuffer crcBuffer, long bytesPerCrc)
      throws IOException {

    if (this.expectedNextOffset == offsetInBlock) {
      long remainingLength = dataLength;
      int oldPosition = crcBuffer.position();
      try {
        while (remainingLength > 0 && canUpdate) {
          int crcB = crcBuffer.getInt();
          long tmpBytesPerCrc = Math.min(remainingLength, bytesPerCrc);
          update(crcB, tmpBytesPerCrc);
          remainingLength -= tmpBytesPerCrc;
        }
        LOG.debug("Successfully update the composite with {} " +
                "data from {}, and bytesPerCrc is {}.",
            dataLength, offsetInBlock, bytesPerCrc);
      } finally {
        crcBuffer.position(oldPosition);
      }
    } else {
      LOG.debug("Will not update the composite crc, because the" +
              " offsetInBlock {} not equal with expected {}.",
          offsetInBlock, this.expectedNextOffset);
    }
  }

  public synchronized void update(int crcB, long bytesPerCrc)
      throws IOException {
    super.update(crcB, bytesPerCrc);
    this.expectedNextOffset += bytesPerCrc;
    verifyAndCloseUpdate(bytesPerCrc);
  }

  private void verifyAndCloseUpdate(long bytesPerCrc) {
    if (bytesPerCrc != getBytesPerCrc()
        || this.expectedNextOffset == this.maxLength) {
      closeUpdate();
    }
  }

  public synchronized void closeUpdate() {
    if (this.canUpdate) {
      this.canUpdate = false;
    }
  }

  public synchronized boolean isClosed() {
    return !canUpdate;
  }

  public synchronized long getLength() {
    return this.expectedNextOffset;
  }

  public synchronized byte[] digest() {
    if (!canUpdate) {
      return digestInternal(false);
    } else {
      LOG.debug("Can't digest because it still can be updated.");
      return null;
    }
  }
}
