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

import org.apache.hadoop.hdfs.net.NetworkTopologyUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * A BlockPlacementPolicy to choose target datanode simply for DecommissionTool.
 * this class does not extend
 * {@link org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy} and does not
 * share interface despite serving a similar purpose.
 */
public class BlockPlacementPolicyForDecommissionTool {
  private final Logger LOG = LoggerFactory.getLogger(BlockPlacementPolicyForDecommissionTool.class);
  private final NetworkTopology clusterMap;
  private final String bpId;
  private final Map<String, DatanodeInfo> dataNodes = new HashMap<>();
  private final Map<String, HashSet<String>> dcRacks = new HashMap<>();

  public BlockPlacementPolicyForDecommissionTool(NetworkTopology clusterMap, String bpId) {
    this.clusterMap = clusterMap;
    this.bpId = bpId;
  }

  public DatanodeInfo getDataNode(String uuid) {
    return dataNodes.get(uuid);
  }

  public boolean isOnSameRack(DatanodeInfo dn1, DatanodeInfo dn2) {
    return this.clusterMap.isOnSameRack(dn1, dn2);
  }

  public void addNode(DatanodeInfo dnInfo) {
    dataNodes.put(dnInfo.getDatanodeUuid(), dnInfo);
    if (dnInfo.isInService()) {
      String dc = NetworkTopologyUtil.getDataCenter(dnInfo);
      HashSet<String> racks = this.dcRacks.computeIfAbsent(dc, k -> new HashSet<>());
      racks.add(dnInfo.getNetworkLocation());
      this.clusterMap.add(dnInfo);
      LOG.info("[{}] Add {} into cluster, uuId {}.", bpId, dnInfo, dnInfo.getDatanodeUuid());
    } else {
      LOG.warn("[{}] Skip this abnormal DN {} with status {}, uuId {}.",
          bpId, dnInfo, dnInfo.getAdminState(), dnInfo.getDatanodeUuid());
    }
  }

  // Case1: S(DC1-R1), Replica2(DC1-R2), Replica3(DC1-R1) => Randomly Choose Replica4(DC1-RX)
  // Case2: S(DC1-R1), Replica2(DC1-R2), Replica3(DC1-R2) => Randomly Choose Replica4(DC1-RX(X != R2))
  // Case3: S(DC1-R1), Replica2(DC1-R2), Replica3(DC2-R3) => Randomly Choose Replica4(DC1-RX)
  // Case4: S(DC1-R1), Replica2(DC2-R2), Replica3(DC2-R3) => Randomly Choose Replica4(DC1-RX)
  public DatanodeInfo chooseTargetForContiguousBlock(Block block, DatanodeInfo source,
      DatanodeInfo target, List<DatanodeInfo> locations, String targetDC) {
    List<Node> excludeNode = new ArrayList<>(locations);
    if (target != null) {
      excludeNode.add(target);
    }

    String expectedDC = targetDC;
    if (expectedDC == null || expectedDC.isEmpty()) {
      expectedDC = NetworkTopologyUtil.getDataCenter(source);
    }

    Map<String, Integer> locationRacks = getLocationRacks(locations, block);
    List<String> racks = getRacks(expectedDC);

    // Remove source rack from locationRacks and racks.
    String sourceRack = source.getNetworkLocation();
    locationRacks.remove(sourceRack);
    racks.remove(sourceRack);

    List<String> preferRacks = new ArrayList<>(locationRacks.keySet());
    if (preferRacks.size() > 0) {
      Collections.shuffle(preferRacks);
    }

    // Choose one datanode from the rack which stores one replica of this block first
    for (String expectedRack : preferRacks) {
      if (expectedRack.startsWith(expectedDC)) {
        int value = locationRacks.get(expectedRack);
        if (value == 1) {
          DatanodeInfo node = (DatanodeInfo) this.clusterMap.chooseRandom(expectedRack, excludeNode);
          if (node != null && !excludeNode.contains(node)) {
            LOG.debug("Choose {} as target node for {} with source is {}.", node, block, source);
            return node;
          }
        }
      }
    }

    // Fallback to the global rack if the target node cannot be chosen from the expected racks.
    for (String rack : racks) {
      int rackCount = locationRacks.getOrDefault(rack, 0);
      if (rackCount != 2) {
        DatanodeInfo node = (DatanodeInfo) this.clusterMap.chooseRandom(rack, excludeNode);
        if (node != null && !excludeNode.contains(node)) {
          LOG.debug("Choose {} as target node for {} with source is {}.", node, block, source);
          return node;
        }
      }
    }

    // Fallback to the source rack.
    DatanodeInfo node = (DatanodeInfo) this.clusterMap.chooseRandom(sourceRack, excludeNode);
    if (node != null && !excludeNode.contains(node)) {
      LOG.debug("Choose {} as target node for {} with source is {}.", node, block, source);
      return node;
    }

    LOG.error("Cannot choose any target node for {} with locations {}.", block, locations);
    return null;
  }

  // Case1: S(DC1-R1), Replica2(DC1-R2), Replica3(DC1-R3) => Randomly Choose Replica4(DC1-RX(X != R2 & X != R3))
  public DatanodeInfo chooseTargetForStripeBlock(
      Block block, DatanodeInfo source, DatanodeInfo target,
      List<DatanodeInfo> locations, String targetDC) {
    List<Node> excludeNode = new ArrayList<>(locations);
    if (target != null) {
      excludeNode.add(target);
    }

    Map<String, Integer> locationRacks = getLocationRacks(locations, block);
    String expectedDC = targetDC;
    if (expectedDC == null || expectedDC.isEmpty()) {
      expectedDC = NetworkTopologyUtil.getDataCenter(source);
    }
    List<String> racks = getRacks(expectedDC);

    for (String rack : racks) {
      if (!locationRacks.containsKey(rack)) {
        DatanodeInfo node = (DatanodeInfo) this.clusterMap.chooseRandom(rack, excludeNode);
        if (node != null && node != source) {
          LOG.debug("Choose {} as target node for {} with source is {}.", node, block, source);
          return node;
        }
      }
    }

    // Fallback to the source rack
    DatanodeInfo node = (DatanodeInfo) this.clusterMap.chooseRandom(
        source.getNetworkLocation(), excludeNode);
    if (node != null && node != source) {
      LOG.debug("Choose {} as target node for {} with source is {}.", node, block, source);
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
      LOG.warn("[{}] block {} locations {}.", this.bpId, block, locations);
      throw e;
    }

    return locationRacks;
  }

  private List<String> getRacks(String expectedDC) {
    List<String> racks;
    if (expectedDC != null) {
      racks = new ArrayList<>(this.dcRacks.get(expectedDC));
    } else {
      racks = new ArrayList<>();
      dcRacks.forEach((k, v) -> racks.addAll(v));
    }
    Collections.shuffle(racks);
    return racks;
  }
}
