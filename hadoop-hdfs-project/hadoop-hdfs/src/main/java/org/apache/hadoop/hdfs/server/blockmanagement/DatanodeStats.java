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

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.net.DFSNetworkTopologyWithDataCenter;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.net.NetworkTopology;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Datanode statistics.
 * For decommissioning/decommissioned nodes, only used capacity is counted.
 */
class DatanodeStats {

  private NetworkTopology networkTopology;
  private final StorageTypeStatsMap statsMap = new StorageTypeStatsMap();
  private final DataCenterStatsMap dataCenterStatsMap =
      new DataCenterStatsMap();
  private long capacityTotal = 0L;
  private long capacityUsed = 0L;
  private long capacityUsedNonDfs = 0L;
  private long capacityRemaining = 0L;
  private long blockPoolUsed = 0L;
  private int xceiverCount = 0;
  private long cacheCapacity = 0L;
  private long cacheUsed = 0L;

  private int nodesInService = 0;
  private int nodesInServiceXceiverCount = 0;
  private int expiredHeartbeats = 0;

  void setNetworkTopology(NetworkTopology networkTopology) {
    this.networkTopology = networkTopology;
  }

  synchronized void add(final DatanodeDescriptor node) {
    xceiverCount += node.getXceiverCount();
    if (node.isInService()) {
      capacityUsed += node.getDfsUsed();
      capacityUsedNonDfs += node.getNonDfsUsed();
      blockPoolUsed += node.getBlockPoolUsed();
      nodesInService++;
      nodesInServiceXceiverCount += node.getXceiverCount();
      capacityTotal += node.getCapacity();
      capacityRemaining += node.getRemaining();
      cacheCapacity += node.getCacheCapacity();
      cacheUsed += node.getCacheUsed();
    } else if (node.isDecommissionInProgress() ||
        node.isEnteringMaintenance()) {
      cacheCapacity += node.getCacheCapacity();
      cacheUsed += node.getCacheUsed();
    }
    Set<StorageType> storageTypes = new HashSet<>();
    for (DatanodeStorageInfo storageInfo : node.getStorageInfos()) {
      if (storageInfo.getState() != DatanodeStorage.State.FAILED) {
        statsMap.addStorage(storageInfo, node);
        storageTypes.add(storageInfo.getStorageType());
      }
    }
    for (StorageType storageType : storageTypes) {
      statsMap.addNode(storageType, node);
    }
    if (networkTopology instanceof DFSNetworkTopologyWithDataCenter) {
      dataCenterStatsMap.addNode(node);
    }
  }

  synchronized void subtract(final DatanodeDescriptor node) {
    xceiverCount -= node.getXceiverCount();
    if (node.isInService()) {
      capacityUsed -= node.getDfsUsed();
      capacityUsedNonDfs -= node.getNonDfsUsed();
      blockPoolUsed -= node.getBlockPoolUsed();
      nodesInService--;
      nodesInServiceXceiverCount -= node.getXceiverCount();
      capacityTotal -= node.getCapacity();
      capacityRemaining -= node.getRemaining();
      cacheCapacity -= node.getCacheCapacity();
      cacheUsed -= node.getCacheUsed();
    } else if (node.isDecommissionInProgress() ||
        node.isEnteringMaintenance()) {
      cacheCapacity -= node.getCacheCapacity();
      cacheUsed -= node.getCacheUsed();
    }
    Set<StorageType> storageTypes = new HashSet<>();
    for (DatanodeStorageInfo storageInfo : node.getStorageInfos()) {
      if (storageInfo.getState() != DatanodeStorage.State.FAILED) {
        statsMap.subtractStorage(storageInfo, node);
        storageTypes.add(storageInfo.getStorageType());
      }
    }
    for (StorageType storageType : storageTypes) {
      statsMap.subtractNode(storageType, node);
    }
    if (networkTopology instanceof DFSNetworkTopologyWithDataCenter) {
      dataCenterStatsMap.subtractNode(node);
    }
  }

  /** Increment expired heartbeat counter. */
  void incrExpiredHeartbeats() {
    expiredHeartbeats++;
  }

  synchronized Map<StorageType, StorageTypeStats> getStatsMap() {
    return statsMap.get();
  }

  synchronized Map<String, DataCenterStats> getDataCenterStatsMap() {
    return dataCenterStatsMap.get();
  }

  synchronized long getCapacityTotal() {
    return capacityTotal;
  }

  synchronized long getCapacityUsed() {
    return capacityUsed;
  }

  synchronized long getCapacityRemaining() {
    return capacityRemaining;
  }

  synchronized long getBlockPoolUsed() {
    return blockPoolUsed;
  }

  synchronized int getXceiverCount() {
    return xceiverCount;
  }

  synchronized long getCacheCapacity() {
    return cacheCapacity;
  }

  synchronized long getCacheUsed() {
    return cacheUsed;
  }

  synchronized int getNodesInService() {
    return nodesInService;
  }

  synchronized int getNodesInServiceXceiverCount() {
    return nodesInServiceXceiverCount;
  }

  synchronized int getDataCenterNodesInService(String dataCenter) {
    if (getDataCenterStatsMap().containsKey(dataCenter)) {
      return getDataCenterStatsMap().get(dataCenter).getNodesInService();
    }
    return 0;
  }

  synchronized int getDataCenterNodesInServiceXceiverCount(String dataCenter) {
    if (getDataCenterStatsMap().containsKey(dataCenter)) {
      return getDataCenterStatsMap().get(dataCenter)
          .getNodesInServiceXceiverCount();
    }
    return 0;
  }

  synchronized int getExpiredHeartbeats() {
    return expiredHeartbeats;
  }

  synchronized float getCapacityRemainingPercent() {
    return DFSUtilClient.getPercentRemaining(capacityRemaining, capacityTotal);
  }

  synchronized float getPercentBlockPoolUsed() {
    return DFSUtilClient.getPercentUsed(blockPoolUsed, capacityTotal);
  }

  synchronized long getCapacityUsedNonDFS() {
    return capacityUsedNonDfs;
  }

  synchronized float getCapacityUsedPercent() {
    return DFSUtilClient.getPercentUsed(capacityUsed, capacityTotal);
  }

  static final class StorageTypeStatsMap {

    private Map<StorageType, StorageTypeStats> storageTypeStatsMap =
        new EnumMap<>(StorageType.class);

    private Map<StorageType, StorageTypeStats> get() {
      return new EnumMap<>(storageTypeStatsMap);
    }

    private void addNode(StorageType storageType,
        final DatanodeDescriptor node) {
      StorageTypeStats storageTypeStats =
          storageTypeStatsMap.get(storageType);
      if (storageTypeStats == null) {
        storageTypeStats = new StorageTypeStats();
        storageTypeStatsMap.put(storageType, storageTypeStats);
      }
      storageTypeStats.addNode(node);
    }

    private void addStorage(final DatanodeStorageInfo info,
        final DatanodeDescriptor node) {
      StorageTypeStats storageTypeStats =
          storageTypeStatsMap.get(info.getStorageType());
      if (storageTypeStats == null) {
        storageTypeStats = new StorageTypeStats();
        storageTypeStatsMap.put(info.getStorageType(), storageTypeStats);
      }
      storageTypeStats.addStorage(info, node);
    }

    private void subtractStorage(final DatanodeStorageInfo info,
        final DatanodeDescriptor node) {
      StorageTypeStats storageTypeStats =
          storageTypeStatsMap.get(info.getStorageType());
      if (storageTypeStats != null) {
        storageTypeStats.subtractStorage(info, node);
      }
    }

    private void subtractNode(StorageType storageType,
        final DatanodeDescriptor node) {
      StorageTypeStats storageTypeStats = storageTypeStatsMap.get(storageType);
      if (storageTypeStats != null) {
        storageTypeStats.subtractNode(node);
        if (storageTypeStats.getNodesInService() == 0) {
          storageTypeStatsMap.remove(storageType);
        }
      }
    }
  }

  static final class DataCenterStatsMap {

    private Map<String, DataCenterStats> dataCenterStatsMap = new HashMap<>();

    private Map<String, DataCenterStats> get() {
      return  new HashMap<>(dataCenterStatsMap);
    }

    private void addNode(final DatanodeDescriptor node) {
      String dc = DFSNetworkTopologyWithDataCenter.getDataCenter(
          node.getNetworkLocation());
      DataCenterStats dataCenterStats = dataCenterStatsMap.get(dc);
      if (dataCenterStats == null) {
        dataCenterStats = new DataCenterStats();
        dataCenterStatsMap.put(dc, dataCenterStats);
      }
      dataCenterStats.addNode(node);
    }

    private void subtractNode(final DatanodeDescriptor node) {
      String dc = DFSNetworkTopologyWithDataCenter.getDataCenter(
          node.getNetworkLocation());
      DataCenterStats dataCenterStats = dataCenterStatsMap.get(dc);
      if (dataCenterStats != null) {
        dataCenterStats.subtractNode(node);
        if (dataCenterStats.getNodesInService() == 0) {
          dataCenterStatsMap.remove(dc);
        }
      }
    }
  }
}
