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

import org.apache.hadoop.hdfs.net.NetworkTopologyUtil;
import org.apache.hadoop.hdfs.server.zoneservice.ReplicationRule;
import org.apache.hadoop.net.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;

class ReplicationWork {
  private final BlockInfo block;
  private final String srcPath;
  private final long blockSize;
  private final byte storagePolicyID;
  private DatanodeDescriptor srcNode;
  private final int additionalReplRequired;
  private final int priority;
  private final List<DatanodeDescriptor> containingNodes;
  private final List<DatanodeStorageInfo> liveReplicaStorages;
  private DatanodeStorageInfo[] targets;
  private final BlockCollection bc;
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationWork.class);

  public ReplicationWork(BlockInfo block, BlockCollection bc,
      DatanodeDescriptor srcNode, List<DatanodeDescriptor> containingNodes,
      List<DatanodeStorageInfo> liveReplicaStorages, int additionalReplRequired,
      int priority) {
    this.block = block;
    this.srcPath = bc.getName();
    this.blockSize = block.getNumBytes();
    this.storagePolicyID = bc.getStoragePolicyID();
    this.srcNode = srcNode;
    this.srcNode.incrementPendingReplicationWithoutTargets();
    this.containingNodes = containingNodes;
    this.liveReplicaStorages = liveReplicaStorages;
    this.additionalReplRequired = additionalReplRequired;
    this.priority = priority;
    this.targets = null;
    this.bc = bc;
  }

  void chooseTargets(BlockPlacementPolicy blockplacement,
      BlockStoragePolicySuite storagePolicySuite,
      Set<Node> excludedNodes) {
    LOG.debug("Try to chooseTarget for blk_" + block.getBlockId());
    try {
      targets = blockplacement.chooseTarget(getSrcPath(),
          additionalReplRequired, srcNode, liveReplicaStorages, false,
          excludedNodes, blockSize,
          storagePolicySuite.getPolicy(getStoragePolicyID()), null);
    } finally {
      srcNode.decrementPendingReplicationWithoutTargets();
    }
  }

  /**
   * Choose targets according to the replication rule.
   * @param blockplacement block placement policy
   * @param storagePolicySuite storage policy suite
   * @param excludedNodes excluded nodes
   * @param rule rule to follow
   */
  void chooseTargets(BlockPlacementPolicy blockplacement,
      BlockStoragePolicySuite storagePolicySuite,
      Set<Node> excludedNodes,
      ReplicationRule rule) {
    LOG.debug("Try to chooseTarget for blk_" + block.getBlockId());
    DatanodeDescriptor originalSrcNode = srcNode;
    try {
      targets = blockplacement.chooseTarget(getSrcPath(),
          additionalReplRequired, rule, srcNode, liveReplicaStorages, false,
          excludedNodes, blockSize,
          storagePolicySuite.getPolicy(getStoragePolicyID()), null);
      if (targets.length > 0) {
        String targetDc = NetworkTopologyUtil.getDataCenter(
            targets[0].getDatanodeDescriptor());
        if (!NetworkTopologyUtil.getDataCenter(srcNode).equals(targetDc)) {
          List<DatanodeStorageInfo> storages = NetworkTopologyUtil.
              getStoragesInDataCenter(liveReplicaStorages, targetDc);
          if (storages.size() > 0) {
            srcNode = storages.get(0).getDatanodeDescriptor();
          }
        }
      }
    } finally {
      originalSrcNode.decrementPendingReplicationWithoutTargets();
    }
  }

  DatanodeStorageInfo[] getTargets() {
    return targets;
  }

  void resetTargets() {
    this.targets = null;
  }

  List<DatanodeDescriptor> getContainingNodes() {
    return Collections.unmodifiableList(containingNodes);
  }

  public int getPriority() {
    return priority;
  }

  public BlockInfo getBlock() {
    return block;
  }

  public DatanodeDescriptor getSrcNode() {
    return srcNode;
  }

  public String getSrcPath() {
    return srcPath;
  }

  public byte getStoragePolicyID() {
    return storagePolicyID;
  }

  public BlockCollection getBlockCollection() {
    return bc;
  }
}
