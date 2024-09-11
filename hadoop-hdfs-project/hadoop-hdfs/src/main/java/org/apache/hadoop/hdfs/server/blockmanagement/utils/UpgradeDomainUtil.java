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
package org.apache.hadoop.hdfs.server.blockmanagement.utils;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy.NotEnoughReplicasException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementStatus;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementStatusDefault;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementStatusWithUpgradeDomain;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyDefault.ONE_RACK_PLACEMENT;

public class UpgradeDomainUtil {
  static final Logger LOG = LoggerFactory.getLogger(UpgradeDomainUtil.class);

  // If upgrade domain isn't specified, uses its XferAddr as upgrade domain.
  // Such fallback is useful to test the scenario where upgrade domain isn't
  // defined but the block placement is set to upgrade domain policy.
  public static String getUpgradeDomainWithDefaultValue(DatanodeInfo datanodeInfo) {
    String upgradeDomain = datanodeInfo.getUpgradeDomain();
    if (upgradeDomain == null) {
      LOG.debug("Upgrade domain isn't defined for {}.", datanodeInfo);
      upgradeDomain = datanodeInfo.getXferAddr();
    }
    LOG.debug("Upgrade domain of {} is {}.", datanodeInfo, upgradeDomain);
    return upgradeDomain;
  }

  public static String getUpgradeDomain(DatanodeStorageInfo storage) {
    return getUpgradeDomainWithDefaultValue(storage.getDatanodeDescriptor());
  }

  public static Set<String> getUpgradeDomains(List<DatanodeStorageInfo> results) {
    Set<String> upgradeDomains = new HashSet<>();
    if (results == null) {
      return upgradeDomains;
    }
    for (DatanodeStorageInfo storageInfo : results) {
      upgradeDomains.add(getUpgradeDomain(storageInfo));
    }
    return upgradeDomains;
  }

  public static Set<String> getUpgradeDomainsForDNs(List<DatanodeInfo> results) {
    Set<String> upgradeDomains = new HashSet<>();
    if (results == null) {
      return upgradeDomains;
    }
    for (DatanodeInfo datanodeInfo : results) {
      upgradeDomains.add(getUpgradeDomainWithDefaultValue(datanodeInfo));
    }
    return upgradeDomains;
  }

  public static Set<String> getUpgradeDomainsFromNodes(DatanodeInfo[] nodes) {
    Set<String> upgradeDomains = new HashSet<>();
    if (nodes == null) {
      return upgradeDomains;
    }
    for (DatanodeInfo node : nodes) {
      upgradeDomains.add(getUpgradeDomainWithDefaultValue(node));
    }
    return upgradeDomains;
  }

  public static <T> Map<String, List<T>> getUpgradeDomainMap(Collection<T> storagesOrDataNodes) {
    Map<String, List<T>> upgradeDomainMap = new HashMap<>();
    for (T storage : storagesOrDataNodes) {
      String upgradeDomain = getUpgradeDomainWithDefaultValue(
          BlockPlacementCommonUtil.getDatanodeInfo(storage));
      List<T> storages = upgradeDomainMap.computeIfAbsent(upgradeDomain, k -> new ArrayList<>());
      storages.add(storage);
    }
    return upgradeDomainMap;
  }

  public static <T> List<T> getShareUDSet(Map<String, List<T>> upgradeDomains) {
    List<T> getShareUDSet = new ArrayList<>();
    for (Map.Entry<String, List<T>> e : upgradeDomains.entrySet()) {
      if (e.getValue().size() > 1) {
        getShareUDSet.addAll(e.getValue());
      }
    }
    return getShareUDSet;
  }

  public static Collection<DatanodeStorageInfo> combine(
      Collection<DatanodeStorageInfo> moreThanOne,
      Collection<DatanodeStorageInfo> exactlyOne) {
    List<DatanodeStorageInfo> all = new ArrayList<>();
    if (moreThanOne != null) {
      all.addAll(moreThanOne);
    }
    if (exactlyOne != null) {
      all.addAll(exactlyOne);
    }
    return all;
  }

  // Check if moving from source to target will preserve the upgrade domain
  // policy.
  public static <T> boolean isMovableBasedOnUpgradeDomain(Collection<T> all,
      T source, T target, int upgradeDomainFactor) {
    Map<String, List<T>> udMap = getUpgradeDomainMap(all);
    // shareUDSet includes dataNodes that share same upgrade
    // domain with another datanode.
    List<T> shareUDSet = getShareUDSet(udMap);
    // check if removing source reduces the number of upgrade domains
    if (BlockPlacementCommonUtil.notReduceNumOfGroups(shareUDSet, source, target)) {
      return true;
    } else {
      return udMap.size() > upgradeDomainFactor;
    }
  }

  /**
   * This method assumes that DNs in results already satisfied UpgradeDomain.
   */
  public static boolean isGoodDataNodeWithUD(boolean isGoodTarget, DatanodeDescriptor node,
      List<DatanodeStorageInfo> results, int upgradeDomainFactor) {
    if (isGoodTarget) {
      if (!results.isEmpty() && results.size() < upgradeDomainFactor) {
        // Each node in "results" has a different upgrade domain. Make sure
        // the candidate node introduces a new upgrade domain.
        Set<String> upgradeDomains = UpgradeDomainUtil.getUpgradeDomains(results);
        if (upgradeDomains.contains(node.getUpgradeDomain())) {
          isGoodTarget = false;
        }
      }
    }
    return isGoodTarget;
  }

  // TODO: Need to take storage type into account via DatanodeInfoWithStorage
  public static DatanodeInfo[] correctBlock(DatanodeInfo[] locs, int upgradeDomainFactor,
      NetworkTopology clusterMap) throws NotEnoughReplicasException {
    // results to be returned
    List<DatanodeInfo> results = new ArrayList<>();
    Set<Node> excludedNodes = new HashSet<>(Arrays.asList(locs));
    if (LOG.isDebugEnabled()) {
      for (DatanodeInfo node : locs) {
        LOG.debug("UpgradeDomain of {} is {}.", node, node.getUpgradeDomain());
      }
    }
    final Set<String> upgradeDomains = new HashSet<>();
    List<DatanodeInfo> toBeReplacedNodes = new ArrayList<>();

    // Sort out the UpgradeDomains of the current Locations.
    Map<String, List<DatanodeInfo>> upgradeDomainMap =
        UpgradeDomainUtil.getUpgradeDomainMap(Arrays.asList(locs));
    for (Map.Entry<String, List<DatanodeInfo>> e : upgradeDomainMap.entrySet()) {
      results.add(e.getValue().get(0));
      for (int i = 1; i < e.getValue().size(); i++) {
        toBeReplacedNodes.add(e.getValue().get(i));
      }
      upgradeDomains.add(e.getKey());
    }

    Function<DatanodeInfo, Boolean> isGoodNodes = input -> {
      assert input != null;
      return !upgradeDomains.contains(UpgradeDomainUtil.getUpgradeDomainWithDefaultValue(input));
    };

    int minUD = Math.min(locs.length, upgradeDomainFactor);
    while (true) {
      // Quickly return if the number of upgradeDomains meets the minimum upgradeDomain factor.
      if (upgradeDomains.size() >= minUD) {
        results.addAll(toBeReplacedNodes);
        break;
      }
      DatanodeInfo toBeReplaceNode = toBeReplacedNodes.get(0);
      String rackName = toBeReplaceNode.getNetworkLocation();

      // Try to choose one node from the local rack.
      DatanodeInfo datanodeInfo = chooseRandomNode(clusterMap, rackName, excludedNodes, isGoodNodes);
      if (datanodeInfo == null) {
        // Try to choose one node from remote rack.
        datanodeInfo = chooseRandomNode(clusterMap, "~" + rackName, excludedNodes, isGoodNodes);
      }

      if (datanodeInfo == null) {
        throw new NotEnoughReplicasException("Can't find any datanode from the cluster.");
      } else {
        results.add(datanodeInfo);
        upgradeDomains.add(UpgradeDomainUtil.getUpgradeDomainWithDefaultValue(datanodeInfo));
        toBeReplacedNodes.remove(0);
      }
    }

    if (LOG.isDebugEnabled()) {
      for (DatanodeInfo node : results) {
        LOG.debug("Finish CorrectBlock and UpgradeDomain of {} is {}.",
            node, node.getUpgradeDomain());
      }
    }
    return results.toArray(DatanodeInfo.EMPTY_ARRAY);
  }

  /**
   * Randomly choose a good node under scope.
   */
  private static DatanodeInfo chooseRandomNode(NetworkTopology clusterMap,
      String scope, Set<Node> excludedNodes, Function<DatanodeInfo, Boolean> isGoodNodes) {
    while (true) {
      Node node = clusterMap.chooseRandom(scope, excludedNodes);
      if (node == null) {
        return null;
      } else {
        excludedNodes.add(node);
        DatanodeInfo datanodeInfo = (DatanodeInfo) node;
        if (isGoodNodes.apply(datanodeInfo)) {
          return datanodeInfo;
        }
      }
    }
  }

  /**
   * Check if locs satisfy block placement.
   */
  public static BlockPlacementStatus verifyBlockPlacement(DatanodeInfo[] locs,
      int numberOfReplicas, NetworkTopology clusterMap, int upgradeDomainFactor) {
    if (locs == null) {
      locs = DatanodeDescriptor.EMPTY_ARRAY;
    }
    if (!clusterMap.hasClusterEverBeenMultiRack()) {
      // only one rack
      return ONE_RACK_PLACEMENT;
    }
    final int minRacks = Math.min(2, numberOfReplicas);
    // 1. Check that all locations are different.
    // 2. Count locations on different racks.
    final long rackCount = Arrays.stream(locs).map(DatanodeInfo::getNetworkLocation)
        .distinct().count();

    BlockPlacementStatus defaultStatus = new BlockPlacementStatusDefault(
        Math.toIntExact(rackCount), minRacks, clusterMap.getNumOfNonEmptyRacks());

    return new BlockPlacementStatusWithUpgradeDomain(defaultStatus,
        UpgradeDomainUtil.getUpgradeDomainsFromNodes(locs), numberOfReplicas, upgradeDomainFactor);
  }
}