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
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.zoneservice.ReplicationRule;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.net.Node;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

class ErasureCodingWork extends BlockReconstructionWork {
  private final byte[] liveBlockIndicies;
  private final byte[] liveBusyBlockIndicies;
  private final byte[] excludeReconstructedIndices;
  private final String blockPoolId;
  private boolean adjustTargetNodes = false;

  public ErasureCodingWork(String blockPoolId, BlockInfo block,
      BlockCollection bc,
      DatanodeDescriptor[] srcNodes,
      List<DatanodeDescriptor> containingNodes,
      List<DatanodeStorageInfo> liveReplicaStorages,
      int additionalReplRequired, int priority,
      byte[] liveBlockIndicies, byte[] liveBusyBlockIndicies,
      byte[] excludeReconstructedIndices, boolean notEnoughRack) {
    super(block, bc, srcNodes, containingNodes,
        liveReplicaStorages, additionalReplRequired, priority);
    this.blockPoolId = blockPoolId;
    this.liveBlockIndicies = liveBlockIndicies;
    this.liveBusyBlockIndicies = liveBusyBlockIndicies;
    this.excludeReconstructedIndices = excludeReconstructedIndices;
    if (notEnoughRack) {
      setNotEnoughRack();
    }
    LOG.info("Creating an ErasureCodingWork to {} reconstruct and notEnoughRack is {}",
        block, notEnoughRack);
  }

  byte[] getLiveBlockIndicies() {
    return liveBlockIndicies;
  }

  @Override
  void chooseTargets(BlockPlacementPolicy blockplacement,
      BlockStoragePolicySuite storagePolicySuite,
      Set<Node> excludedNodes, ReplicationRule rule) {
    // TODO: new placement policy for EC considering multiple writers
    DatanodeStorageInfo[] chosenTargets = null;
    // HDFS-14720. If the block is deleted, the block size will become
    // BlockCommand.NO_ACK (LONG.MAX_VALUE) . This kind of block we don't need
    // to send for replication or reconstruction
    if (!getBlock().isDeleted()) {
      if (rule != null) {
        DatanodeDescriptor source;
        if (hasNotEnoughRack()) {
          // If there are not enough racks, choose a source for simple replication.
          source = getSrcNodes()[chooseSource4SimpleReplication()];
        } else {
          source = getSrcNodes()[0];
        }
        chosenTargets = blockplacement.chooseTarget(
            getSrcPath(), getAdditionalReplRequired(), rule, source,
            getLiveReplicaStorages(), false,
            excludedNodes, getBlockSize(),
            storagePolicySuite.getPolicy(getStoragePolicyID()), null, hasNotEnoughRack());
        setAdjustTargetNodes(true);
      } else {
        chosenTargets = blockplacement.chooseTarget(
            getSrcPath(), getAdditionalReplRequired(), getSrcNodes()[0],
            getLiveReplicaStorages(), false, excludedNodes, getBlockSize(),
            storagePolicySuite.getPolicy(getStoragePolicyID()), null);
      }
    } else {
      LOG.warn("ErasureCodingWork could not need choose targets for {}", getBlock());
    }
    setTargets(chosenTargets);
  }

  public void setAdjustTargetNodes(boolean adjustTargetNodes) {
    this.adjustTargetNodes = adjustTargetNodes;
  }

  /**
   * @return true if the current source nodes cover all the internal blocks.
   * I.e., we only need to have more racks.
   */
  private boolean hasAllInternalBlocks() {
    final BlockInfoStriped block = (BlockInfoStriped) getBlock();
    if (liveBlockIndicies.length
        + liveBusyBlockIndicies.length < block.getRealTotalBlockNum()) {
      return false;
    }
    BitSet bitSet = new BitSet(block.getTotalBlockNum());
    for (byte index : liveBlockIndicies) {
      bitSet.set(index);
    }
    for (byte busyIndex: liveBusyBlockIndicies) {
      bitSet.set(busyIndex);
    }
    for (int i = 0; i < block.getRealDataBlockNum(); i++) {
      if (!bitSet.get(i)) {
        return false;
      }
    }
    for (int i = block.getDataBlockNum(); i < block.getTotalBlockNum(); i++) {
      if (!bitSet.get(i)) {
        return false;
      }
    }
    return true;
  }

  /**
   * We have all the internal blocks but not enough racks. Thus we do not need
   * to do decoding but only simply make an extra copy of an internal block. In
   * this scenario, use this method to choose the source datanode for simple
   * replication.
   * @return The index of the source datanode.
   */
  private int chooseSource4SimpleReplication() {
    Map<String, List<Integer>> map = new HashMap<>();
    for (int i = 0; i < getSrcNodes().length; i++) {
      final String rack = getSrcNodes()[i].getNetworkLocation();
      List<Integer> dnList = map.get(rack);
      if (dnList == null) {
        dnList = new ArrayList<>();
        map.put(rack, dnList);
      }
      dnList.add(i);
    }
    List<Integer> max = null;
    for (Map.Entry<String, List<Integer>> entry : map.entrySet()) {
      if (max == null || entry.getValue().size() > max.size()) {
        max = entry.getValue();
      }
    }
    assert max != null;
    return max.get(0);
  }

  @Override
  boolean addTaskToDatanode(NumberReplicas numberReplicas) {
    DatanodeStorageInfo[] targets = getTargets();
    assert targets.length > 0;
    BlockInfoStriped stripedBlk = (BlockInfoStriped) getBlock();
    boolean flag = true;
    if (hasNotEnoughRack()) {
      // if we already have all the internal blocks, but not enough racks,
      // we only need to replicate one internal block to a new rack
      int sourceIndex = chooseSource4SimpleReplication();
      createReplicationWork(sourceIndex, targets[0]);
    } else if ((numberReplicas.decommissioning() > 0 ||
        numberReplicas.liveEnteringMaintenanceReplicas() > 0) &&
        hasAllInternalBlocks()) {
      List<Integer> leavingServiceSources = findLeavingServiceSources();
      if (adjustTargetNodes) {
        List<DatanodeDescriptor> sourceNodes = new ArrayList<>(
            Arrays.asList(getSrcNodes()).subList(0, leavingServiceSources.size()));
        targets = adjustTargetNodes(sourceNodes);
      }
      // decommissioningSources.size() should be >= targets.length
      final int num = Math.min(leavingServiceSources.size(), targets.length);
      if (num == 0) {
        flag = false;
      }
      for (int i = 0; i < num; i++) {
        createReplicationWork(leavingServiceSources.get(i), targets[i]);
      }
    } else {
      targets[0].getDatanodeDescriptor().addBlockToBeErasureCoded(
          new ExtendedBlock(blockPoolId, stripedBlk), getSrcNodes(), targets,
          liveBlockIndicies, excludeReconstructedIndices, stripedBlk.getErasureCodingPolicy());
    }
    return flag;
  }

  private void createReplicationWork(int sourceIndex,
      DatanodeStorageInfo target) {
    BlockInfoStriped stripedBlk = (BlockInfoStriped) getBlock();
    final byte blockIndex = liveBlockIndicies[sourceIndex];
    final DatanodeDescriptor source = getSrcNodes()[sourceIndex];
    final long internBlkLen = StripedBlockUtil.getInternalBlockLength(
        stripedBlk.getNumBytes(), stripedBlk.getCellSize(),
        stripedBlk.getDataBlockNum(), blockIndex);
    final Block targetBlk = new Block(stripedBlk.getBlockId() + blockIndex,
        internBlkLen, stripedBlk.getGenerationStamp());
    source.addECBlockToBeReplicated(targetBlk,
        new DatanodeStorageInfo[] {target});
    LOG.debug("Add replication task from source {} to "
        + "target {} for EC block {}", source, target, targetBlk);
  }

  private List<Integer> findLeavingServiceSources() {
    // Mark the block in normal node.
    BlockInfoStriped block = (BlockInfoStriped)getBlock();
    BitSet bitSet = new BitSet(block.getRealTotalBlockNum());
    for (int i = 0; i < getSrcNodes().length; i++) {
      if (getSrcNodes()[i].isInService()) {
        bitSet.set(liveBlockIndicies[i]);
      }
    }
    // If the block is on the node which is decommissioning or
    // entering_maintenance, and it doesn't exist on other normal nodes,
    // we just add the node into source list.
    List<Integer> srcIndices = new ArrayList<>();
    for (int i = 0; i < getSrcNodes().length; i++) {
      if ((getSrcNodes()[i].isDecommissionInProgress() ||
          (getSrcNodes()[i].isEnteringMaintenance() &&
          getSrcNodes()[i].isAlive())) &&
          !bitSet.get(liveBlockIndicies[i])) {
        srcIndices.add(i);
      }
    }
    return srcIndices;
  }

  /**
   * Adjusts the target nodes based on the given source nodes.
   * the method selects target storage nodes from the targets
   * based on the matching data center of the source nodes.
   * ensure that the copied data remains within the
   * same data center during the decommissioning of datanodes.
   *
   * @param sourceNodes The list of source datanodes.
   * @return An array of adjusted target storage nodes.
   */
  private DatanodeStorageInfo[] adjustTargetNodes(List<DatanodeDescriptor> sourceNodes) {
    DatanodeStorageInfo[] originalTargets = getTargets();
    if (sourceNodes.isEmpty() || originalTargets.length == 0) {
      return DatanodeStorageInfo.EMPTY_ARRAY;
    }
    int targetsLength = originalTargets.length;
    DatanodeStorageInfo[] targets = new DatanodeStorageInfo[targetsLength];
    Map<String, Deque<DatanodeStorageInfo>> idcToStorageMap = new HashMap<>();
    for (DatanodeStorageInfo storage : originalTargets) {
      String idc = NetworkTopologyUtil.getDataCenter(storage.getDatanodeDescriptor());
      idcToStorageMap.computeIfAbsent(idc, k -> new ArrayDeque<>()).add(storage);
    }

    int i = 0;
    for (DatanodeDescriptor datanodeDescriptor : sourceNodes) {
      if (i >= targetsLength) {
        break;
      }
      String idc = NetworkTopologyUtil.getDataCenter(datanodeDescriptor);
      Deque<DatanodeStorageInfo> storageDeque = idcToStorageMap.get(idc);

      if (storageDeque != null && !storageDeque.isEmpty()) {
        targets[i] = storageDeque.poll();
        if (storageDeque.isEmpty()) {
          idcToStorageMap.remove(idc);
        }
      } else if (!idcToStorageMap.isEmpty()) {
        // If no matching IDC found, assign the first available element from any IDC.
        Map.Entry<String, Deque<DatanodeStorageInfo>> entry =
            idcToStorageMap.entrySet().iterator().next();
        targets[i] = entry.getValue().pollLast();
        if (entry.getValue().isEmpty()) {
          idcToStorageMap.remove(entry.getKey());
        }
      }
      i++;
    }
    return targets;
  }

}
