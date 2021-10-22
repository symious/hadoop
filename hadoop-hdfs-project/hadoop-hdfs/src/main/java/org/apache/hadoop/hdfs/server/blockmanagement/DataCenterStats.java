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

package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.beans.ConstructorProperties;

/**
 * Statistics per DataCenter
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DataCenterStats {
  private long capacityTotal = 0L;
  private long capacityUsed = 0L;
  private long capacityNonDfsUsed = 0L;
  private long capacityRemaining = 0L;
  private long blockPoolUsed = 0L;
  private int nodesInService = 0;
  private int nodesInServiceXceiverCount = 0;
  private long cacheCapacity = 0L;
  private long cacheUsed = 0L;

  @ConstructorProperties({"capacityTotal", "capacityUsed", "capacityNonDfsUsed",
      "capacityRemaining", "blockPoolUsed", "nodesInService",
      "nodesInServiceXceiverCount", "cacheCapacity", "cacheused"
  })
  public DataCenterStats(
      long capacityTotal, long capacityUsed, long capacityNonDfsUsed,
      long capacityRemaining, long blockPoolUsed, int nodesInService,
      int nodesInServiceXceiverCount, long cacheCapacity, long cacheUsed) {
    this.capacityTotal = capacityTotal;
    this.capacityUsed = capacityUsed;
    this.capacityNonDfsUsed = capacityNonDfsUsed;
    this.capacityRemaining = capacityRemaining;
    this.blockPoolUsed = blockPoolUsed;
    this.nodesInService = nodesInService;
    this.nodesInServiceXceiverCount = nodesInServiceXceiverCount;
    this.cacheCapacity = cacheCapacity;
    this.cacheUsed = cacheUsed;
  }

  public long getCapacityTotal() {
    return capacityTotal;
  }

  public long getCapacityUsed() {
    return capacityUsed;
  }

  public long getCapacityNonDfsUsed() {
    return capacityNonDfsUsed;
  }

  public long getCapacityRemaining() {
    return capacityRemaining;
  }

  public long getBlockPoolUsed() {
    return blockPoolUsed;
  }

  public int getNodesInService() {
    return nodesInService;
  }

  public int getNodesInServiceXceiverCount() {
    return nodesInServiceXceiverCount;
  }

  DataCenterStats() {}

  DataCenterStats(DataCenterStats other) {
    capacityTotal = other.capacityTotal;
    capacityUsed = other.capacityUsed;
    capacityNonDfsUsed = other.capacityNonDfsUsed;
    capacityRemaining = other.capacityRemaining;
    blockPoolUsed = other.blockPoolUsed;
    nodesInService = other.nodesInService;
    nodesInServiceXceiverCount = other.nodesInServiceXceiverCount;
    cacheCapacity = other.cacheCapacity;
    cacheUsed = other.cacheUsed;
  }

  void addNode(final DatanodeDescriptor node) {
    if (node.isInService()) {
      nodesInService++;
      nodesInServiceXceiverCount += node.getXceiverCount();
      capacityUsed += node.getDfsUsed();
      capacityNonDfsUsed += node.getNonDfsUsed();
      blockPoolUsed += node.getBlockPoolUsed();
      capacityTotal += node.getCapacity();
      capacityRemaining += node.getRemaining();
      cacheCapacity += node.getCacheCapacity();
      cacheUsed += node.getCacheUsed();
    } else if (node.isDecommissionInProgress() ||
        node.isEnteringMaintenance()) {
      cacheCapacity += node.getCacheCapacity();
      cacheUsed += node.getCacheUsed();
    }
  }

  void subtractNode(final DatanodeDescriptor node) {
    if (node.isInService()) {
      nodesInService--;
      nodesInServiceXceiverCount -= node.getXceiverCount();
      capacityUsed -= node.getDfsUsed();
      capacityNonDfsUsed -= node.getNonDfsUsed();
      blockPoolUsed -= node.getBlockPoolUsed();
      capacityTotal -= node.getCapacity();
      capacityRemaining -= node.getRemaining();
      cacheCapacity -= node.getCacheCapacity();
      cacheUsed -= node.getCacheUsed();
    } else if (node.isDecommissionInProgress() ||
        node.isEnteringMaintenance()) {
      cacheCapacity -= node.getCacheCapacity();
      cacheUsed -= node.getCacheUsed();
    }
  }
}
