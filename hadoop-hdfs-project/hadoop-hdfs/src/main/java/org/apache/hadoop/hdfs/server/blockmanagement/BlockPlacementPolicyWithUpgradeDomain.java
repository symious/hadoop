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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.utils.UpgradeDomainUtil;
import org.apache.hadoop.net.NetworkTopology;

/**
 * The class is responsible for choosing the desired number of targets
 * for placing block replicas that honors upgrade domain policy.
 * Here is the replica placement strategy. If the writer is on a datanode,
 * the 1st replica is placed on the local machine,
 * otherwise a random datanode. The 2nd replica is placed on a datanode
 * that is on a different rack. The 3rd replica is placed on a datanode
 * which is on a different node of the rack as the second replica.
 * All 3 replicas have unique upgrade domains.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BlockPlacementPolicyWithUpgradeDomain extends
    BlockPlacementPolicyDefault {

  private int upgradeDomainFactor;

  @Override
  public void initialize(Configuration conf,  FSClusterStats stats,
      NetworkTopology clusterMap, Host2NodesMap host2datanodeMap) {
    super.initialize(conf, stats, clusterMap, host2datanodeMap);
    upgradeDomainFactor = conf.getInt(
        DFSConfigKeys.DFS_UPGRADE_DOMAIN_FACTOR,
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
  public BlockPlacementStatus verifyBlockPlacement(DatanodeInfo[] locs,
      int numberOfReplicas) {
    BlockPlacementStatus defaultStatus = super.verifyBlockPlacement(locs,
        numberOfReplicas);
    return new BlockPlacementStatusWithUpgradeDomain(defaultStatus,
        UpgradeDomainUtil.getUpgradeDomainsFromNodes(locs), numberOfReplicas,
        upgradeDomainFactor);
  }

  /*
   * The policy to pick the replica set for deleting the over-replicated
   * replica which meet the rack and upgrade domain requirements.
   * The algorithm:
   * a. Each replica has a boolean attribute "shareRack" that defines
   *    whether it shares its rack with another replica of the same block.
   * b. Each replica has another boolean attribute "shareUD" that defines
   *    whether it shares its upgrade domain with another replica of the same
   *    block.
   * c. Partition the replicas into 4 sets (some might be empty.):
   *    shareRackAndUDSet: {shareRack==true, shareUD==true}
   *    shareUDNotRackSet: {shareRack==false, shareUD==true}
   *    shareRackNotUDSet: {shareRack==true, shareUD==false}
   *    NoShareRackOrUDSet: {shareRack==false, shareUD==false}
   * d. Pick the first not-empty replica set in the following order.
   *    shareRackAndUDSet, shareUDNotRackSet, shareRackNotUDSet,
   *    NoShareRackOrUDSet
   * e. Proof this won't degrade the existing rack-based data
   *    availability model under different scenarios.
   *    1. shareRackAndUDSet isn't empty. Removing a node
   *       from shareRackAndUDSet won't change # of racks and # of UD.
   *       The followings cover empty shareUDNotRackSet scenarios.
   *    2. shareUDNotRackSet isn't empty and shareRackNotUDSet isn't empty.
   *       Let us proof that # of racks >= 3 before the deletion and thus
   *       after deletion # of racks >= 2.
   *         Given shareUDNotRackSet is empty, there won't be overlap between
   *       shareUDNotRackSet and shareRackNotUDSet. It means DNs in
   *       shareRackNotUDSet should be on at least a rack
   *       different from any DN' rack in shareUDNotRackSet.
   *         Given shareUDNotRackSet.size() >= 2 and each DN in the set
   *       doesn't share rack with any other DNs, there are at least 2 racks
   *       coming from shareUDNotRackSet.
   *         Thus the # of racks from DNs in {shareUDNotRackSet,
   *       shareRackNotUDSet} >= 3. Removing a node from shareUDNotRackSet
   *       will reduce the # of racks by 1 and won't change # of upgrade
   *       domains.
   *         Note that this is similar to BlockPlacementPolicyDefault which
   *       will at most reduce the # of racks by 1, and never reduce it to < 2.
   *       With upgrade domain policy, given # of racks is still >= 2 after
   *       deletion, the data availability model remains the same as
   *       BlockPlacementPolicyDefault (only supports one rack failure).
   *         For example, assume we have 4 replicas: d1(rack1, ud1),
   *       d2(rack2, ud1), d3(rack3, ud3), d4(rack3, ud4). Thus we have
   *       shareUDNotRackSet: {d1, d2} and shareRackNotUDSet: {d3, d4}.
   *       With upgrade domain policy, the remaining replicas after deletion
   *       are {d1(or d2), d3, d4} which has 2 racks.
   *       With BlockPlacementPolicyDefault policy, any of the 4 with the worst
   *       condition will be deleted, which in worst case has 2 racks remain.
   *    3. shareUDNotRackSet isn't empty and shareRackNotUDSet is empty. This
   *       implies all replicas are on unique racks. Removing a node from
   *       shareUDNotRackSet will reduce # of racks (no different from
   *       BlockPlacementPolicyDefault) by 1 and won't change #
   *       of upgrade domains.
   *    4. shareUDNotRackSet is empty and shareRackNotUDSet isn't empty.
   *       Removing a node from shareRackNotUDSet is no different from
   *       BlockPlacementPolicyDefault.
   *    5. shareUDNotRackSet is empty and shareRackNotUDSet is empty.
   *       Removing a node from NoShareRackOrUDSet is no different from
   *       BlockPlacementPolicyDefault.
   * The implementation:
   * 1. Generate set shareUDSet which includes all DatanodeStorageInfo that
   *    share the same upgrade domain with another DatanodeStorageInfo,
   *    e.g. {shareRackAndUDSet, shareUDNotRackSet}.
   * 2. If shareUDSet is empty, it means shareRackAndUDSet is empty and
   *    shareUDNotRackSet is empty. Use the default rack based policy.
   * 3. If shareUDSet isn't empty, intersect it with moreThanOne(
   *    {shareRackAndUDSet, shareRackNotUDSet})to generate shareRackAndUDSet.
   * 4. If shareRackAndUDSet isn't empty, return
   *    shareRackAndUDSet, otherwise return shareUDSet which is the same as
   *    shareUDNotRackSet.
   */
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
    return shareRackAndUDSet.isEmpty() ? shareUDSet : shareRackAndUDSet;
  }

  @Override
  boolean useDelHint(DatanodeStorageInfo delHint,
      DatanodeStorageInfo added, List<DatanodeStorageInfo> moreThanOne,
      Collection<DatanodeStorageInfo> exactlyOne,
      List<StorageType> excessTypes) {
    if (!super.useDelHint(delHint, added, moreThanOne, exactlyOne,
        excessTypes)) {
      // If BlockPlacementPolicyDefault doesn't allow useDelHint, there is no
      // point checking with upgrade domain policy.
      return false;
    }
    return UpgradeDomainUtil.isMovableBasedOnUpgradeDomain(
        UpgradeDomainUtil.combine(moreThanOne, exactlyOne),
        delHint, added, upgradeDomainFactor);
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
