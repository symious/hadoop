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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.utils.UpgradeDomainUtil;
import org.apache.hadoop.net.NetworkTopology;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class BlockPlacementPolicyWithUpgradeDomainForDataCenter
    extends BlockPlacementPolicyWithDataCenter {

  private int upgradeDomainFactor;

  @Override
  public void initialize(Configuration conf,  FSClusterStats stats,
      NetworkTopology clusterMap, Host2NodesMap host2datanodeMap) {
    super.initialize(conf, stats, clusterMap, host2datanodeMap);
    upgradeDomainFactor = conf.getInt(DFSConfigKeys.DFS_UPGRADE_DOMAIN_FACTOR,
        DFSConfigKeys.DFS_UPGRADE_DOMAIN_FACTOR_DEFAULT);
  }

  @Override
  protected boolean isGoodDatanode(DatanodeDescriptor node,
      int maxTargetPerRack, boolean considerLoad,
      List<DatanodeStorageInfo> results, boolean avoidStaleNodes) {
    boolean isGoodTarget = super.isGoodDatanode(node,
        maxTargetPerRack, considerLoad, results, avoidStaleNodes);
    return UpgradeDomainUtil.isGoodDataNodeWithUD(isGoodTarget, node, results, upgradeDomainFactor);
  }

  @Override
  public BlockPlacementStatus verifyBlockPlacement(DatanodeInfo[] locs, int numberOfReplicas) {
    BlockPlacementStatus defaultStatus = super.verifyBlockPlacement(locs, numberOfReplicas);
    return new BlockPlacementStatusWithUpgradeDomain(defaultStatus,
        UpgradeDomainUtil.getUpgradeDomainsFromNodes(locs), numberOfReplicas, upgradeDomainFactor);
  }

  @Override
  protected Collection<DatanodeStorageInfo> pickupReplicaSet(
      Collection<DatanodeStorageInfo> moreThanOne,
      Collection<DatanodeStorageInfo> exactlyOne,
      Map<String, List<DatanodeStorageInfo>> rackMap) {
    // shareUDSet includes DatanodeStorageInfo that share same upgrade
    // domain with another DatanodeStorageInfo.
    Collection<DatanodeStorageInfo> all = UpgradeDomainUtil.combine(moreThanOne, exactlyOne);
    List<DatanodeStorageInfo> shareUDSet = UpgradeDomainUtil.getShareUDSet(
        UpgradeDomainUtil.getUpgradeDomainMap(all));
    // shareRackAndUDSet contains those DatanodeStorageInfo that
    // share rack and upgrade domain with another DatanodeStorageInfo.
    List<DatanodeStorageInfo> shareRackAndUDSet = new ArrayList<>();
    if (shareUDSet.isEmpty()) {
      // All upgrade domains are unique, use the parent set.
      return super.pickupReplicaSet(moreThanOne, exactlyOne, rackMap);
    } else if (moreThanOne != null) {
      for (DatanodeStorageInfo storage : shareUDSet) {
        if (moreThanOne.contains(storage)) {
          shareRackAndUDSet.add(storage);
        }
      }
    }
    return !shareRackAndUDSet.isEmpty() ? shareRackAndUDSet : shareUDSet;
  }

  @Override
  boolean useDelHint(DatanodeStorageInfo delHint,
      DatanodeStorageInfo added, List<DatanodeStorageInfo> moreThanOne,
      Collection<DatanodeStorageInfo> exactlyOne,
      List<StorageType> excessTypes) {
    if (!super.useDelHint(delHint, added, moreThanOne, exactlyOne, excessTypes)) {
      // If BlockPlacementPolicyDefault doesn't allow useDelHint, there is no
      // point checking with upgrade domain policy.
      return false;
    }
    return UpgradeDomainUtil.isMovableBasedOnUpgradeDomain(
        UpgradeDomainUtil.combine(moreThanOne, exactlyOne), delHint, added, upgradeDomainFactor);
  }

  @Override
  public boolean isMovable(Collection<DatanodeInfo> locs,
      DatanodeInfo source, DatanodeInfo target) {
    if (super.isMovable(locs, source, target)) {
      return UpgradeDomainUtil.isMovableBasedOnUpgradeDomain(
          locs, source, target, upgradeDomainFactor);
    } else {
      return false;
    }
  }
}