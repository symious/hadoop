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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.net.DFSNetworkTopologyWithDataCenter;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;

import java.util.Collection;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * This class is responsible for choosing the desired number of targets
 * for placing block replicas on an environment with datacenter awareness.
 *
 * The replica placement strategy is adjusted to:
 * If the writer is on a datanode, the 1st replica is placed on the local
 *  machine, otherwise a random datanode in the same data center with the
 *  writer.
 * The 2nd replica is placed on a datanode that is on a different rack in
 *  the same data center.
 * The 3rd replica is placed on a datanode that is on a different node of
 *  the rack as the second replica.
 */
@InterfaceAudience.Private
public class BlockPlacementPolicyWithDataCenter extends
    BlockPlacementPolicyDefault {

  protected DFSNetworkTopologyWithDataCenter dcClusterMap;

  @Override
  public void initialize(Configuration conf,
      FSClusterStats stats, NetworkTopology clusterMap,
      Host2NodesMap host2datanodeMap) {
    if (clusterMap instanceof DFSNetworkTopologyWithDataCenter) {
      this.dcClusterMap = (DFSNetworkTopologyWithDataCenter) clusterMap;
    } else {
      throw new IllegalArgumentException("BlockPlacementPolicyWithDataCenter " +
          "must work with DFSNetworkTopologyWithDataCenter!");
    }
    super.initialize(conf, stats, clusterMap, host2datanodeMap);
  }

  @Override
  protected Node chooseTargetInOrder(int numOfReplicas, Node writer,
      final Set<Node> excludedNodes, final long blockSize,
      final int maxNodesPerRack, final List<DatanodeStorageInfo> results,
      final boolean avoidStaleNodes, final boolean newBlock,
      EnumMap<StorageType, Integer> storageTypes)
      throws NotEnoughReplicasException {
    final int numOfResults = results.size();
    if (numOfResults == 0) {
      DatanodeStorageInfo storageInfo = chooseLocalStorage(writer, excludedNodes, blockSize,
          maxNodesPerRack, results, avoidStaleNodes,
          storageTypes, true);
      writer = (storageInfo != null) ?
          storageInfo.getDatanodeDescriptor() : null;
      if (--numOfReplicas == 0) {
        return writer;
      }
    }
    final DatanodeDescriptor dn0 = results.get(0).getDatanodeDescriptor();
    if (numOfResults <= 1) {
      chooseRemoteRack(1, dn0, excludedNodes,
          blockSize, maxNodesPerRack, results, avoidStaleNodes, storageTypes);
      if (--numOfReplicas == 0) {
        return writer;
      }
    }
    if (numOfResults <= 2) {
      final DatanodeDescriptor dn1 = results.get(1).getDatanodeDescriptor();
      if (clusterMap.isOnSameRack(dn0, dn1)) {
        chooseRemoteRack(1, dn0, excludedNodes, blockSize,
            maxNodesPerRack, results, avoidStaleNodes, storageTypes);
      } else if (newBlock){
        chooseLocalRack(dn1, excludedNodes, blockSize, maxNodesPerRack,
            results, avoidStaleNodes, storageTypes);
      } else {
        chooseLocalRack(writer, excludedNodes, blockSize, maxNodesPerRack,
            results, avoidStaleNodes, storageTypes);
      }
      if (--numOfReplicas == 0) {
        return writer;
      }
    }
    chooseRandom(writer, numOfReplicas, NodeBase.ROOT, excludedNodes,
        blockSize, maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    return writer;
  }

  @Override
  protected DatanodeStorageInfo chooseLocalStorage(Node localMachine,
      Set<Node> excludedNodes, long blockSize, int maxNodesPerRack,
      List<DatanodeStorageInfo> results, boolean avoidStaleNodes,
      EnumMap<StorageType, Integer> storageTypes)
      throws NotEnoughReplicasException {
    // if no local machine, randomly choose one node
    if (localMachine == null) {
      return chooseRandom(null, 1, NodeBase.ROOT,
          excludedNodes, blockSize, maxNodesPerRack, results,
          avoidStaleNodes, storageTypes);
    }
    if (preferLocalNode && localMachine instanceof DatanodeDescriptor
        && clusterMap.contains(localMachine)) {
      DatanodeDescriptor localDatanode = (DatanodeDescriptor) localMachine;
      // otherwise try local machine first
      if (excludedNodes.add(localMachine) // was not in the excluded list
          && isGoodDatanode(localDatanode, maxNodesPerRack, false,
          results, avoidStaleNodes)) {
        for (Iterator<Map.Entry<StorageType, Integer>> iter = storageTypes
            .entrySet().iterator(); iter.hasNext(); ) {
          Map.Entry<StorageType, Integer> entry = iter.next();
          DatanodeStorageInfo localStorage = chooseStorage4Block(
              localDatanode, blockSize, results, entry.getKey());
          if (localStorage != null) {
            // add node and related nodes to excludedNode
            addToExcludedNodes(localDatanode, excludedNodes);
            int num = entry.getValue();
            if (num == 1) {
              iter.remove();
            } else {
              entry.setValue(num - 1);
            }
            return localStorage;
          }
        }
      }
    }
    return null;
  }

  @Override
  protected DatanodeStorageInfo chooseLocalRack(
      Node localMachine, Set<Node> excludedNodes,
      long blockSize, int maxNodesPerRack,
      List<DatanodeStorageInfo> results,
      boolean avoidStaleNodes,
      EnumMap<StorageType, Integer> storageTypes)
      throws NotEnoughReplicasException {
    // no local machine, so choose a random machine
    if (localMachine == null) {
      return chooseRandom(null, 1, NodeBase.ROOT, excludedNodes,
          blockSize, maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    }
    final String localRack = localMachine.getNetworkLocation();

    try {
      // choose one from the local rack
      return chooseRandom(localMachine, 1, localRack, excludedNodes,
          blockSize, maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    } catch (NotEnoughReplicasException e) {
      // find the next replica and retry with its rack
      for(DatanodeStorageInfo resultStorage : results) {
        DatanodeDescriptor nextNode = resultStorage.getDatanodeDescriptor();
        if (nextNode != localMachine) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Failed to choose from local rack (location = " + localRack
                + "), retry with the rack of the next replica (location = "
                + nextNode.getNetworkLocation() + ")", e);
          }
          return chooseFromNextRack(nextNode, excludedNodes, blockSize,
              maxNodesPerRack, results, avoidStaleNodes, storageTypes);
        }
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Failed to choose from local rack (location = " + localRack
            + "); the second replica is not found, retry choosing ramdomly", e);
      }
      //the second replica is not found, randomly choose one from the network
      return chooseRandom(localMachine, 1, NodeBase.ROOT, excludedNodes,
          blockSize, maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    }
  }

  @Override
  protected DatanodeStorageInfo chooseFromNextRack(
      Node next, Set<Node> excludedNodes, long blockSize,
      int maxNodesPerRack, List<DatanodeStorageInfo> results,
      boolean avoidStaleNodes, EnumMap<StorageType, Integer> storageTypes)
      throws NotEnoughReplicasException {
    final String nextRack = next.getNetworkLocation();
    try {
      return chooseRandom(next, 1, nextRack, excludedNodes, blockSize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    } catch(NotEnoughReplicasException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Failed to choose from the next rack (location = " + nextRack
            + "), retry choosing ramdomly", e);
      }
      //otherwise randomly choose one from the network
      return chooseRandom(next, 1, NodeBase.ROOT, excludedNodes,
          blockSize, maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    }
  }

  @Override
  protected void chooseRemoteRack(
      int numOfReplicas, DatanodeDescriptor localMachine,
      Set<Node> excludedNodes, long blockSize,
      int maxReplicasPerRack, List<DatanodeStorageInfo> results,
      boolean avoidStaleNodes, EnumMap<StorageType, Integer> storageTypes)
      throws NotEnoughReplicasException {
    int oldNumOfReplicas = results.size();
    // randomly choose one node from remote racks
    try {
      chooseRandom(localMachine, numOfReplicas,
          "~" + localMachine.getNetworkLocation(),
          excludedNodes, blockSize, maxReplicasPerRack, results,
          avoidStaleNodes, storageTypes);
    } catch (NotEnoughReplicasException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Failed to choose remote rack (location = ~"
            + localMachine.getNetworkLocation() + "), fallback to local rack", e);
      }
      chooseRandom(localMachine,
          numOfReplicas - (results.size() - oldNumOfReplicas),
          localMachine.getNetworkLocation(), excludedNodes, blockSize,
          maxReplicasPerRack, results, avoidStaleNodes, storageTypes);
    }
  }

  /**
   * Randomly choose one target from the given <i>base</i> and <i>scope</i>.
   * @param base the writer or any allocated nodes
   * @return the first chosen node, if there is any.
   */
  protected DatanodeStorageInfo chooseRandom(
      Node base, int numOfReplicas, String scope,
      Set<Node> excludedNodes, long blockSize,
      int maxNodesPerRack, List<DatanodeStorageInfo> results,
      boolean avoidStaleNodes, EnumMap<StorageType, Integer> storageTypes)
      throws NotEnoughReplicasException {

    if ((base == null) || !scope.equals(NodeBase.ROOT)) {
      if (base == null) {
        LOG.warn("(base.loc={}, base.name={}," +
                " numOfReplicas={}, scope={}, results.size={}.",
             null, null,
            numOfReplicas, scope, results.size());
      } else {
        LOG.warn("(base.loc={}, base.name={}," +
                " numOfReplicas={}, scope={}, results.size={}.",
            base.getNetworkLocation(), base.getName(),
            numOfReplicas, scope, results.size());
      }

      return chooseRandom(numOfReplicas, scope, excludedNodes, blockSize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    }
    String newScope = DFSNetworkTopologyWithDataCenter.getDataCenter(
        base.getNetworkLocation());
    LOG.debug("(base.loc={}, base.name={}," +
            " numOfReplicas={}, scope={}, results.size={}.",
        base.getNetworkLocation(), base.getName(),
        numOfReplicas, newScope, results.size());
    return chooseRandom(numOfReplicas, newScope, excludedNodes, blockSize,
        maxNodesPerRack, results, avoidStaleNodes, storageTypes);
  }

  @Override
  protected DatanodeDescriptor chooseDataNode(
      final String scope, final Collection<Node> excludedNodes) {
    return (DatanodeDescriptor) dcClusterMap
        .chooseRandom(scope, excludedNodes);
  }

  @Override
  protected DatanodeDescriptor chooseDataNode(final String scope,
      final Collection<Node> excludedNodes, StorageType type) {
    return (DatanodeDescriptor) dcClusterMap
        .chooseRandomWithStorageTypeTwoTrial(
            scope, excludedNodes, type);
  }
}