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
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.zoneservice.ReplicationRule;
import org.apache.hadoop.net.Node;

import java.util.List;
import java.util.Set;

class ReplicationWork extends BlockReconstructionWork {
  public ReplicationWork(BlockInfo block, BlockCollection bc,
      DatanodeDescriptor[] srcNodes, List<DatanodeDescriptor> containingNodes,
      List<DatanodeStorageInfo> liveReplicaStorages, int additionalReplRequired,
      int priority) {
    super(block, bc, srcNodes, containingNodes,
        liveReplicaStorages, additionalReplRequired, priority);
    assert getSrcNodes().length == 1 :
        "There should be exactly 1 source node that have been selected";
    getSrcNodes()[0].incrementPendingReplicationWithoutTargets();
    LOG.debug("Creating a ReplicationWork to reconstruct " + block);
  }

  /**
   * According to IDC, and select some datanodes one by one.
   * So all datanode in current targets will in one datacenter.
   */
  @Override
  void chooseTargets(BlockPlacementPolicy blockplacement,
      BlockStoragePolicySuite storagePolicySuite,
      Set<Node> excludedNodes, ReplicationRule rule) {
    LOG.debug("Try to chooseTarget for blk_{}.", getBlock());
    assert getSrcNodes().length > 0
        : "At least 1 source node should have been selected";
    DatanodeDescriptor originalSrcNode = getSrcNodes()[0];
    try {
      DatanodeStorageInfo[] chosenTargets = null;
      // HDFS-14720 If the block is deleted, the block size will become
      // BlockCommand.NO_ACK (LONG.MAX_VALUE) . This kind of block we don't need
      // to send for replication or reconstruction
      if (getBlock().getNumBytes() != BlockCommand.NO_ACK) {
        if (rule != null) {
          chosenTargets = chooseTargetWithDataCenter(blockplacement,
              storagePolicySuite, excludedNodes, rule, originalSrcNode);
        } else {
          chosenTargets = blockplacement.chooseTarget(getSrcPath(),
              getAdditionalReplRequired(), getSrcNodes()[0],
              getLiveReplicaStorages(), false, excludedNodes, getBlockSize(),
              storagePolicySuite.getPolicy(getStoragePolicyID()), null);
        }
      }
      setTargets(chosenTargets);
    } finally {
      originalSrcNode.decrementPendingReplicationWithoutTargets();
    }
  }

  private DatanodeStorageInfo[] chooseTargetWithDataCenter(
      BlockPlacementPolicy blockplacement,
      BlockStoragePolicySuite storagePolicySuite,
      Set<Node> excludedNodes, ReplicationRule rule,
      DatanodeDescriptor originalSrcNode) {
    DatanodeStorageInfo[] chosenTargets = blockplacement.chooseTarget(
        getSrcPath(), getAdditionalReplRequired(), rule, originalSrcNode,
        getLiveReplicaStorages(), false,
        excludedNodes, getBlockSize(),
        storagePolicySuite.getPolicy(getStoragePolicyID()), null);
    if (chosenTargets.length > 0) {
      String targetDc = NetworkTopologyUtil.getDataCenter(
          chosenTargets[0].getDatanodeDescriptor());
      String srcDc = NetworkTopologyUtil.getDataCenter(originalSrcNode);
      if (!srcDc.equals(targetDc)) {
        List<DatanodeStorageInfo> storages = NetworkTopologyUtil.
            getStoragesInDataCenter(getLiveReplicaStorages(), targetDc);
        if (storages.size() > 0) {
          // Reset the source node
          getSrcNodes()[0] = storages.get(0).getDatanodeDescriptor();
          LOG.debug("Changed srcNode from {} to {}",
              originalSrcNode.getIpAddr(), getSrcNodes()[0].getIpAddr());
        }
      }
    }
    return chosenTargets;
  }

  @Override
  void addTaskToDatanode(NumberReplicas numberReplicas) {
    getSrcNodes()[0].addBlockToBeReplicated(getBlock(), getTargets());
  }
}
