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
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.utils.UpgradeDomainUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class BlockPlacementPolicyWithUpgradeDomainForRackFaultTolerantDataCenter
    extends BlockPlacementPolicyRackFaultTolerantDataCenter {

  @Override
  protected boolean isGoodDatanode(DatanodeDescriptor node, int maxTargetPerRack,
      boolean considerLoad, List<DatanodeStorageInfo> results, boolean avoidStaleNodes) {
    boolean isGoodTarget = super.isGoodDatanode(node,
        maxTargetPerRack, considerLoad, results, avoidStaleNodes);
    // Here just ensure all dataNodes are belongs to different UD.
    // TODO: consider the number of UD is not enough.
    return UpgradeDomainUtil.isGoodDataNodeWithUD(isGoodTarget, node, results,
        (results.size() + 1));
  }

  @Override
  public BlockPlacementStatus verifyBlockPlacement(DatanodeInfo[] locs, int numberOfReplicas) {
    BlockPlacementStatus defaultStatus = super.verifyBlockPlacement(locs, numberOfReplicas);
    // Here just ensure all dataNodes are belongs to different UD.
    // TODO: consider the number of UD is not enough.
    return new BlockPlacementStatusWithUpgradeDomain(defaultStatus,
        UpgradeDomainUtil.getUpgradeDomainsFromNodes(locs), numberOfReplicas, numberOfReplicas);
  }

  /**
   * Pick up replica node set for deleting replica as over-replicated.
   * The moreThanOne contains datanodes that share some same racks,
   * and exactlyOne contains datanodes located in different racks.
   * moreThanOne is empty means that all DNs already satisfied rack distribution,
   * all DNs share same UpgradeDomains should be returned.
   * moreThanOne is not empty and shareUDSet is not empty,
   * we need to pick up DNs that share both racks and upgrade domains.
   */
  @Override
  protected Collection<DatanodeStorageInfo> pickupReplicaSet(
      Collection<DatanodeStorageInfo> moreThanOne,
      Collection<DatanodeStorageInfo> exactlyOne,
      Map<String, List<DatanodeStorageInfo>> rackMap) {
    Collection<DatanodeStorageInfo> all = UpgradeDomainUtil.combine(moreThanOne, exactlyOne);
    List<DatanodeStorageInfo> shareUDSet = UpgradeDomainUtil.getShareUDSet(
        UpgradeDomainUtil.getUpgradeDomainMap(all));

    // This means that all DNs satisfy rack distribution.
    if (moreThanOne.isEmpty()) {
      if (shareUDSet.isEmpty()) {
        return exactlyOne;
      } else {
        return shareUDSet;
      }
    } else { // This means that there are some DNs don't satisfy rack distribution.
      if (shareUDSet.isEmpty()) {
        return moreThanOne;
      } else {
        List<DatanodeStorageInfo> shareRackAndUDSet = new ArrayList<>();
        for (DatanodeStorageInfo storage : shareUDSet) {
          if (moreThanOne.contains(storage)) {
            shareRackAndUDSet.add(storage);
          }
        }

        return shareRackAndUDSet.isEmpty() ? moreThanOne : shareRackAndUDSet;
      }
    }
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
    Collection<DatanodeStorageInfo> allDNs = UpgradeDomainUtil.combine(moreThanOne, exactlyOne);
    return UpgradeDomainUtil.isMovableBasedOnUpgradeDomain(allDNs, delHint, added, allDNs.size());
  }

  @Override
  public boolean isMovable(Collection<DatanodeInfo> locs,
      DatanodeInfo source, DatanodeInfo target) {
    if (super.isMovable(locs, source, target)) {
      return UpgradeDomainUtil.isMovableBasedOnUpgradeDomain(locs, source, target, locs.size());
    } else {
      return false;
    }
  }
}
