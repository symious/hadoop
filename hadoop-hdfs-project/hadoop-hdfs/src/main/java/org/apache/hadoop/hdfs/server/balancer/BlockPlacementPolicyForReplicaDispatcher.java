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
package org.apache.hadoop.hdfs.server.balancer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.net.NetworkTopologyUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.utils.UpgradeDomainUtil;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A class used to select good DNs to satisfy block placement.
 * This policy groups all DataNodes according to some dimensions,
 * such as: DataCenter, StorageType, Racks, DataNodes, so that this policy can
 * find a good DataNode quickly and efficiently.
 */
public class BlockPlacementPolicyForReplicaDispatcher {
  private final Logger LOG = LoggerFactory.getLogger(BlockPlacementPolicyForReplicaDispatcher.class);

  private final NameNodeConnector nnc;
  private final String nsId;
  private final Map<String, DatanodeInfo> dataNodes = new HashMap<>();
  /** StorageType -> clusterMap **/
  private final Map<StorageType, NetworkTopology> storageClusterMaps = new HashMap<>();
  /** StorageType -> {DC -> Racks} **/
  private final Map<StorageType, Map<String, HashSet<String>>> storageDCRacks = new HashMap<>();
  private final Configuration conf;
  private final boolean enableUpgradeDomain;

  public BlockPlacementPolicyForReplicaDispatcher(NameNodeConnector nnc,
      Configuration conf, boolean enableUpgradeDomain) throws IOException {
    this.nnc = nnc;
    this.conf = conf;
    this.nsId = this.nnc.getNsId();
    this.enableUpgradeDomain = enableUpgradeDomain;
    init();
  }

  private void init() throws IOException {
    final List<DatanodeStorageReport> reports = Arrays.asList(
        nnc.getLiveDatanodeStorageReport());
    Collections.shuffle(reports);
    for (DatanodeStorageReport r : reports) {
      addNode(r);
    }
  }

  /**
   * Build clusterMaps.
   */
  private void addNode(DatanodeStorageReport report) {
    DatanodeInfo dnInfo = report.getDatanodeInfo();
    if (dnInfo.isInService()) { // skip unhealthy nodes.
      for (StorageType t : StorageType.getMovableTypes()) {
        if (isContainStorageType(report, t)) {
          dataNodes.put(toKey(dnInfo.getDatanodeUuid(), t), dnInfo);
          NetworkTopology networkTopology = storageClusterMaps.computeIfAbsent(t,
              k -> NetworkTopology.getInstance(this.conf));
          networkTopology.add(dnInfo);

          String dc = NetworkTopologyUtil.getDataCenter(dnInfo);
          Map<String, HashSet<String>> dcRacks = storageDCRacks.computeIfAbsent(t,
              k -> new HashMap<>());
          HashSet<String> racks = dcRacks.computeIfAbsent(dc, k -> new HashSet<>());
          racks.add(dnInfo.getNetworkLocation());
          LOG.info("[{}] Add {} into cluster for {}, uuId {}.",
              nsId, dnInfo, t, dnInfo.getDatanodeUuid());
        }
      }
    } else {
      LOG.warn("[{}] Skip this abnormal DN {} with status {}, uuId {}.",
          nsId, dnInfo, dnInfo.getAdminState(), dnInfo.getDatanodeUuid());
    }
  }

  /**
   * return true if the report contains the storage type.
   */
  private boolean isContainStorageType(DatanodeStorageReport report, StorageType t) {
    for (StorageReport rp : report.getStorageReports()) {
      if (rp.getStorage().getStorageType() == t) {
        return true;
      }
    }
    return false;
  }

  /**
   * Return datanode info stored in clusterMap.
   */
  public DatanodeInfo getDataNode(String datanodeUuid, StorageType storageType) {
    return dataNodes.get(toKey(datanodeUuid, storageType));
  }

  private String toKey(String datanodeUuid, StorageType storageType) {
    return datanodeUuid + ":" + storageType;
  }

  /**
   * A target data node is selected to store a replica migrated from the source data node.
   * @param block the block need to be migrated
   * @param source the source datanode
   * @param storageType the source storage type
   * @param target old target datanode
   * @param locations a list of dataNodes storing this block
   * @param targetDC prefer dataCenter
   */
  public DatanodeInfo chooseTargetForContiguousBlock(Block block,
      DatanodeInfo source, StorageType storageType, DatanodeInfo target,
      List<DatanodeInfo> locations, String targetDC, List<Node> excludedNodes) {
    if (target != null) {
      excludedNodes.add(target);
    }

    List<DatanodeInfo> existingNodesExcludeSrc = new ArrayList<>(locations);
    existingNodesExcludeSrc.remove(source);

    String preferDC = targetDC;
    if (preferDC == null || preferDC.isEmpty()) {
      preferDC = NetworkTopologyUtil.getDataCenter(source);
    }

    Map<String, Integer> locationRacks = getLocationRacks(locations, block);

    // Remove source rack from locationRacks and racks.
    String sourceRack = source.getNetworkLocation();
    locationRacks.remove(sourceRack);

    // Choose one datanode from the rack which stores one replica of this block first
    List<String> preferRacks = new ArrayList<>(locationRacks.keySet());
    if (!preferRacks.isEmpty()) {
      Collections.shuffle(preferRacks);
    }
    for (String preferRack : preferRacks) {
      if (preferRack.startsWith(preferDC)) {
        int value = locationRacks.get(preferRack);
        // It means that the current rack already stores one replica.
        // We can choose a new DN from this Rack to store another replica.
        if (value == 1) {
          DatanodeInfo node = chooseDNFromRack(storageType, preferRack,
              excludedNodes, existingNodesExcludeSrc);
          if (node != null) {
            return node;
          }
        }
      }
    }

    // Fallback to the global rack if the target node cannot be chosen from the expected racks.
    List<String> allRacks = getRacks(storageType, preferDC);
    allRacks.remove(sourceRack);
    for (String rack : allRacks) {
      int rackCount = locationRacks.getOrDefault(rack, 0);
      if (rackCount != 2) {
        DatanodeInfo node = chooseDNFromRack(storageType,
            rack, excludedNodes, existingNodesExcludeSrc);
        if (node != null) {
          return node;
        }
      }
    }

    // Fallback to the source rack.
    DatanodeInfo node = chooseDNFromRack(storageType,
        sourceRack, excludedNodes, existingNodesExcludeSrc);
    if (node != null) {
      return node;
    }

    LOG.error("Cannot choose any target node for {} with locations {}.", block, locations);
    return null;
  }

  /**
   * Choose a DN from the input Rack.
   */
  private DatanodeInfo chooseDNFromRack(StorageType storageType, String rack,
      List<Node> excludeNode, List<DatanodeInfo> existingNodesExcludeSrc) {
    while (true) {
      DatanodeInfo node = (DatanodeInfo) this.storageClusterMaps
          .get(storageType).chooseRandom(rack, excludeNode);
      // No available node in this rack.
      if (node == null) {
        LOG.debug("Cannot choose a new DN from {} with excludeNodes {}" +
            " and existingNodesExcludeSrc {}.", rack, excludeNode, existingNodesExcludeSrc);
        return null;
      } else if (isGoodDatanode(node, existingNodesExcludeSrc)) {
        excludeNode.add(node);
        return node;
      } else {
        // The chosen node is not a good node.
        excludeNode.add(node);
      }
    }
  }

  /**
   * Return ture if this target node is a good node.
   */
  private boolean isGoodDatanode(DatanodeInfo targetNode, List<DatanodeInfo> results) {
    if (!enableUpgradeDomain) {
      return true;
    } else { // TODO: support UpgradeDomain after rebased mater
      // Just consider that the new DN exists in a new UpgradeDomain.
      Set<String> upgradeDomains = UpgradeDomainUtil.getUpgradeDomainsForDNs(results);
      return !upgradeDomains.contains(
          UpgradeDomainUtil.getUpgradeDomainWithDefaultValue(targetNode));
    }
  }

  /**
   * Choose datanode for stripe block.
   */
  public DatanodeInfo chooseTargetForStripeBlock(Block block,
      DatanodeInfo source, StorageType storageType, DatanodeInfo target,
      List<DatanodeInfo> locations, String targetDC, List<Node> excludeNodes) {
    if (target != null) {
      excludeNodes.add(target);
    }
    List<DatanodeInfo> existingNodesExcludeSrc = new ArrayList<>(locations);
    existingNodesExcludeSrc.remove(source);

    Map<String, Integer> locationRacks = getLocationRacks(locations, block);
    String preferDC = targetDC;
    if (preferDC == null || preferDC.isEmpty()) {
      preferDC = NetworkTopologyUtil.getDataCenter(source);
    }
    List<String> allRacks = getRacks(storageType, preferDC);
    for (String rack : allRacks) {
      // Skip used racks first
      if (!locationRacks.containsKey(rack)) {
        DatanodeInfo node = chooseDNFromRack(storageType, rack, excludeNodes,
            existingNodesExcludeSrc);
        if (node != null) {
          return node;
        }
      }
    }

    // Fallback to the source rack
    DatanodeInfo node = chooseDNFromRack(storageType, source.getNetworkLocation(),
        excludeNodes, existingNodesExcludeSrc);
    if (node != null) {
      return node;
    }

    LOG.error("Cannot choose any target node for {} with locations {}.", block, locations);
    return null;
  }

  private Map<String, Integer> getLocationRacks(List<DatanodeInfo> locations, Block block) {
    Map<String, Integer> locationRacks = new HashMap<>();
    try {
      for (DatanodeInfo dn : locations) {
        String rack = dn.getNetworkLocation();
        int value = locationRacks.getOrDefault(rack, 0);
        locationRacks.put(rack, ++value);
      }
    } catch (NullPointerException e) {
      LOG.warn("[{}] block {} locations {}.", this.nsId, block, locations);
      throw e;
    }

    return locationRacks;
  }

  /**
   * Return all racks for storage type and data center.
   */
  private List<String> getRacks(StorageType storageType, String dataCenter) {
    List<String> racks = new ArrayList<>(
        this.storageDCRacks.get(storageType).get(dataCenter));
    Collections.shuffle(racks);
    return racks;
  }
}
