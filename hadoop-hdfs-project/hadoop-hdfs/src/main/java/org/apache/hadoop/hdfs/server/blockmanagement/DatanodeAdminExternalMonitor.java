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

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.namenode.INodeId;
import org.apache.hadoop.hdfs.util.CyclicIteration;
import org.apache.hadoop.hdfs.util.LightWeightLinkedSet;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class DatanodeAdminExternalMonitor extends DatanodeAdminDefaultMonitor {
  private final TreeMap<DatanodeDescriptor, Integer> outOfServiceNodeBlocksNum;
  private final HashMap<DatanodeDescriptor, Long> outOfServiceNode2StartTime;

  /**
   * The last datanode in outOfServiceNodeBlocks that we've processed.
   */
  private DatanodeDescriptor iterkey = new DatanodeDescriptor(
      new DatanodeID("", "", "", 0, 0, 0, 0));

  private static final Logger LOG =
      LoggerFactory.getLogger(DatanodeAdminExternalMonitor.class);

  public DatanodeAdminExternalMonitor() {
    outOfServiceNodeBlocksNum = new TreeMap<>();
    outOfServiceNode2StartTime = new HashMap<>();
  }

  private volatile long timeThreshold =
      DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_EXTERNAL_MONITOR_TIME_THRESHOLD_DEFAULT;
  private volatile long numberThreshold =
      DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_EXTERNAL_MONITOR_NUMBER_THRESHOLD_DEFAULT;

  @Override
  protected void processPendingNodes() {
    LOG.info("The number of pending nodes is {}", getPendingNodes().size());
    while (!getPendingNodes().isEmpty() &&
        (maxConcurrentTrackedNodes == 0 ||
            outOfServiceNodeBlocksNum.size() < maxConcurrentTrackedNodes)) {
      DatanodeDescriptor dn = getPendingNodes().poll();
      outOfServiceNodeBlocksNum.put(dn, -1);
      outOfServiceNode2StartTime.put(dn, Time.monotonicNow());
    }
  }

  @Override
  protected void processCancelledNodes() {
    while(!getCancelledNodes().isEmpty()) {
      DatanodeDescriptor dn = getCancelledNodes().poll();
      outOfServiceNodeBlocksNum.remove(dn);
      outOfServiceNode2StartTime.remove(dn);
    }
  }

  @Override
  protected void check() {
    final Iterator<Map.Entry<DatanodeDescriptor, Integer>> it =
        new CyclicIteration<>(outOfServiceNodeBlocksNum, iterkey).iterator();
    final List<DatanodeDescriptor> toRemove = new ArrayList<>();
    final List<DatanodeDescriptor> unhealthyDns = new ArrayList<>();

    while (it.hasNext() && !exceededNumBlocksPerCheck() && namesystem.isRunning()) {
      numNodesChecked++;
      final Map.Entry<DatanodeDescriptor, Integer> entry = it.next();
      final DatanodeDescriptor dn = entry.getKey();
      try {
        Integer blocksNum = entry.getValue();
        boolean fullScan = false;
        if (dn.isMaintenance() && dn.maintenanceExpired()) {
          // If maintenance expires, stop tracking it.
          dnAdmin.stopMaintenance(dn);
          toRemove.add(dn);
          continue;
        }
        if (dn.isInMaintenance()) {
          // The dn is IN_MAINTENANCE and the maintenance hasn't expired yet.
          continue;
        }
        if (blocksNum < 0) {
          // This is a newly added datanode, run through its list to schedule
          // under-replicated blocks for replication and collect the blocks
          // that are insufficiently replicated for further tracking
          LOG.info("Newly-added node {}, doing full scan to find " +
              "insufficiently-replicated blocks.", dn);
          blocksNum = dn.numBlocks();
          outOfServiceNodeBlocksNum.put(dn, blocksNum);
          fullScan = true;
        } else {
          // This is a known datanode, check if its # of insufficiently
          // replicated blocks has dropped to zero and if it can move
          // to the next state.
          LOG.info("Processing {} node {}", dn.getAdminState(), dn);
          if (blocksNum == dn.numBlocks() && blocksNum != 0) {
            LOG.warn("The blocks number of Datanode {} hasn't changed, block number is {}!", dn,
                blocksNum);
          }
        }
        dn.getLeavingServiceStatus().set(0,
            new LightWeightLinkedSet<>(), blocksNum, 0);
        final boolean isHealthy = blockManager.isNodeHealthyForDecommissionOrMaintenance(dn);
        if (!isHealthy) {
          unhealthyDns.add(dn);
        }

        // Always do the full scan to confirm the DN block number change
        if (!fullScan) {
          // If we didn't just do a full scan, need to re-check with the
          // full block map.
          //
          // We've replicated all the known insufficiently replicated
          // blocks. Re-check with the full block map before finally
          // marking the datanode as DECOMMISSIONED or IN_MAINTENANCE.
          LOG.info("Node {} has finished replicating current set of "
              + "blocks, checking with the full block map.", dn);
          blocksNum = dn.numBlocks();
          outOfServiceNodeBlocksNum.put(dn, blocksNum);
        }

        if (blocksNum == 0 || allBlocksSatisfyPolicy(dn, outOfServiceNode2StartTime.get(dn))) {
          // If the full scan is clean AND the node liveness is okay,
          // we can finally mark as DECOMMISSIONED or IN_MAINTENANCE.
          if (isHealthy) {
            if (dn.isDecommissionInProgress()) {
              dnAdmin.setDecommissioned(dn);
              toRemove.add(dn);
            } else if (dn.isEnteringMaintenance()) {
              // IN_MAINTENANCE node remains in the outOfServiceNodeBlocks to
              // to track maintenance expiration.
              dnAdmin.setInMaintenance(dn);
            } else {
              Preconditions.checkState(false,
                  "Node %s is in an invalid state! "
                      + "Invalid state: %s %s blocks are on this dn.",
                  dn, dn.getAdminState(), blocksNum);
            }
            LOG.debug("Node {} is sufficiently replicated and healthy, "
                + "marked as {}.", dn, dn.getAdminState());
          } else {
            LOG.info("Node {} {} healthy."
                    + " It needs to replicate {} more blocks."
                    + " {} is still in progress.", dn,
                isHealthy ? "is": "isn't", blocksNum, dn.getAdminState());
          }
        } else {
          LOG.info("Node {} still has {} blocks to replicate "
                  + "before it is a candidate to finish {}.",
              dn, blocksNum, dn.getAdminState());
        }
      } catch (Exception e) {
        // Log and postpone to process node when meet exception since it is in
        // an invalid state.
        LOG.warn("DatanodeAdminMonitor caught exception when processing node "
            + "{}.", dn, e);
        getPendingNodes().add(dn);
        toRemove.add(dn);
        unhealthyDns.remove(dn);
      } finally {
        iterkey = dn;
      }
    }
    // Having more nodes decommissioning than can be tracked will impact decommissioning
    // performance due to queueing delay
    int numTrackedNodes = outOfServiceNodeBlocksNum.size() - toRemove.size();
    int numQueuedNodes = getPendingNodes().size();
    int numDecommissioningNodes = numTrackedNodes + numQueuedNodes;
    if (numDecommissioningNodes > maxConcurrentTrackedNodes) {
      LOG.warn(
          "{} nodes are decommissioning but only {} nodes will be tracked at a time. "
              + "{} nodes are currently queued waiting to be decommissioned.",
          numDecommissioningNodes, maxConcurrentTrackedNodes, numQueuedNodes);

      // Re-queue unhealthy nodes to make space for decommissioning healthy nodes
      getUnhealthyNodesToRequeue(unhealthyDns, numDecommissioningNodes).forEach(dn -> {
        getPendingNodes().add(dn);
        outOfServiceNodeBlocksNum.remove(dn);
        outOfServiceNode2StartTime.remove(dn);
      });
    }
    // Remove the datanodes that are DECOMMISSIONED or in service after
    // maintenance expiration.
    for (DatanodeDescriptor dn : toRemove) {
      Preconditions.checkState(dn.isDecommissioned() || dn.isInService(),
          "Removing node %s that is not yet decommissioned or in service!",
          dn);
      outOfServiceNodeBlocksNum.remove(dn);
      outOfServiceNode2StartTime.remove(dn);
    }
  }

  @Override
  protected void processConf() {
    super.processConf();
    setTimeThreshold(conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_EXTERNAL_MONITOR_TIME_THRESHOLD_KEY,
        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_EXTERNAL_MONITOR_TIME_THRESHOLD_DEFAULT));
    setNumberThreshold(conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_EXTERNAL_MONITOR_NUMBER_THRESHOLD_KEY,
        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_EXTERNAL_MONITOR_NUMBER_THRESHOLD_DEFAULT));
    LOG.info("Initialized the External Decommission and Maintenance monitor");
  }

  public void setTimeThreshold(long newValue) {
    if (newValue <= 0) {
      LOG.error("{} must be greater than zero. Defaulting to {}",
          DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_EXTERNAL_MONITOR_TIME_THRESHOLD_KEY,
          DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_EXTERNAL_MONITOR_TIME_THRESHOLD_DEFAULT);
      newValue = DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_EXTERNAL_MONITOR_TIME_THRESHOLD_DEFAULT;
    }
    LOG.info("Changing the time threshold from {} to {} for {}.", timeThreshold, newValue,
        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_EXTERNAL_MONITOR_TIME_THRESHOLD_KEY);
    timeThreshold = newValue;
  }

  public void setNumberThreshold(long newValue) {
    if (newValue <= 0) {
      LOG.error("{} must be greater than zero. Defaulting to {}",
          DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_EXTERNAL_MONITOR_NUMBER_THRESHOLD_KEY,
          DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_EXTERNAL_MONITOR_NUMBER_THRESHOLD_DEFAULT);
      newValue = DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_EXTERNAL_MONITOR_NUMBER_THRESHOLD_DEFAULT;
    }
    LOG.info("Changing the number threshold from {} to {} for {}.", numberThreshold, newValue,
        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_EXTERNAL_MONITOR_NUMBER_THRESHOLD_KEY);
    numberThreshold = newValue;
  }

  @Override
  public boolean isTrackingNode(DatanodeDescriptor dn) {
    return outOfServiceNodeBlocksNum.containsKey(dn) || getPendingNodes().contains(dn);
  }

  /**
   * Only check whether this DN can be decommissioned or inMaintenance if it has been
   * processed for a long time and the number of remaining blocks is less than a certain threshold.
   * @return true if this DN can be decommissioned or inMaintenance.
   */
  @VisibleForTesting
  public boolean allBlocksSatisfyPolicy(DatanodeDescriptor dn, long beginTime) {
    if (dn.numBlocks() > numberThreshold || (Time.monotonicNow() - beginTime) < timeThreshold) {
      return false;
    }

    // Check whether all the remaining block meet the block storage placement policy.
    for (Iterator<BlockInfo> it = dn.getBlockIterator(); it.hasNext(); ) {
      numBlocksChecked++;
      BlockInfo block = it.next();
      BlockInfo storedBlock = blockManager.blocksMap.getStoredBlock(block);
      if (storedBlock == null) {
        LOG.warn("This block {} in {} is leaked.", block, dn);
        continue;
      }
      long bcId = block.getBlockCollectionId();
      if (bcId == INodeId.INVALID_INODE_ID) {
        // Orphan block, will be invalidated eventually. Skip.
        continue;
      }

      if (storedBlock.isComplete()) {
        final NumberReplicas num = blockManager.countNodes(block);
        if (blockManager.hasEnoughEffectiveReplicas(block, num, 0)) {
          // Block has enough replica, can continue to check the next block.
          LOG.trace("Block {} does not need replication.", block);
        } else {
          LOG.info("Block {} in {} still needs more replication.", block, dn);
          // means that namenode can not change the state to `decommissioned` for this DN.
          return false;
        }
      } else {
        if (block.getGenerationStamp() < storedBlock.getGenerationStamp()) {
          // Block has a stale GS, can continue to check the next block.
          LOG.trace("Block {} has a old GS {}, the current GS is {}.",
              block, block.getGenerationStamp(), storedBlock.getGenerationStamp());
        } else {
          LOG.info("Block {} in {} is still in RBW.", block, dn);
          // means that namenode can not change the state to `decommissioned` for this DN.
          return false;
        }
      }
    }
    return true;
  }
}
