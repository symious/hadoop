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
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.hdfs.net.DFSNetworkTopologyWithDataCenter;
import org.apache.hadoop.hdfs.net.NetworkTopologyUtil;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.server.zoneservice.ReplicationRule;
import org.apache.hadoop.hdfs.server.zoneservice.ReplicationRuleSection;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
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
  // use a virtual base to limit scope to the target datacenter
  private static final Map<String, Node> dcBaseNodes = new HashMap<>();
  private static final String VIRTUAL_HOST = "virtual_host";
  private static final String VIRTUAL_RACK = "/virtual_rack";
  private static final List<DatanodeStorageInfo> EMPTY_NODES = Collections.emptyList();

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

    // If the local machine is in the excludedNodes, return null.
    // this avoids an unnecessary call to clusterMap.contains(localMachine) in subsequent logic,
    // preventing potential performance issues.
    if (excludedNodes.contains(localMachine)) {
      return null;
    }

    if (preferLocalNode && localMachine instanceof DatanodeDescriptor
        && clusterMap.contains(localMachine)) {
      DatanodeDescriptor localDatanode = (DatanodeDescriptor) localMachine;
      // otherwise try local machine first
      if (excludedNodes.add(localMachine) // was not in the excluded list
          && isGoodDatanode(localDatanode, maxNodesPerRack, considerLoad,
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
    if (localRack.endsWith(VIRTUAL_RACK)) {
      return chooseRandom(localMachine, 1, NodeBase.ROOT, excludedNodes,
          blockSize, maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    }

    try {
      // choose one from the local rack
      return chooseRandom(localMachine, 1, localRack, excludedNodes,
          blockSize, maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    } catch (NotEnoughReplicasException e) {
      // find the next replica and retry with its rack
      String localDataCenter = NetworkTopologyUtil.getDataCenter(localMachine);
      for(DatanodeStorageInfo resultStorage : results) {
        DatanodeDescriptor nextNode = resultStorage.getDatanodeDescriptor();
        String nextDataCenter = NetworkTopologyUtil.getDataCenter(nextNode);
        if ((nextNode != localMachine) && (localDataCenter.equals(nextDataCenter))) {
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
        LOG.debug("(base.loc={}, base.name={}, numOfReplicas={}, scope={}, results.size={}.",
            base.getNetworkLocation(), base.getName(),
            numOfReplicas, scope, results.size());
      }

      return chooseRandom(numOfReplicas, scope, excludedNodes, blockSize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    }
    String newScope = NetworkTopologyUtil.getDataCenter(base);
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

  @Override
  protected double getInServiceXceiverAverage(DatanodeDescriptor node) {
    double inServiceXceiverCount;
    String dataCenter = NetworkTopologyUtil.getDataCenter(node);
    inServiceXceiverCount = stats.getDataCenterInServiceXceiverAverage(dataCenter);
    return inServiceXceiverCount;
  }

  @Override
  public List<DatanodeStorageInfo> chooseReplicasToDelete(
      Collection<DatanodeStorageInfo> availableReplicas,
      Collection<DatanodeStorageInfo> delCandidates,
      int expectedNumOfReplicas, ReplicationRule rule,
      List<StorageType> excessTypes, DatanodeDescriptor addedNode,
      DatanodeDescriptor delNodeHint) {
    // Replicas are in one datacenter.
    if (rule.getSections().size() == 1) {
      return super.chooseReplicasToDelete(availableReplicas, delCandidates,
          expectedNumOfReplicas, excessTypes, addedNode, delNodeHint);
    }

    List<DatanodeStorageInfo> excessReplicas = new ArrayList<>();
    final Map<String, List<DatanodeStorageInfo>> dcMap = new HashMap<>();

    splitNodesWithDataCenter(delCandidates, dcMap);

    // Handle the replicas which are not included in the rule.
    for (String dcName : dcMap.keySet()) {
      if (!rule.getDatacenters().contains(dcName)) {
        List<DatanodeStorageInfo> storageInfos = dcMap.get(dcName);
        boolean full = addExcessReplicas(delCandidates, expectedNumOfReplicas,
            storageInfos, excessReplicas, delNodeHint);
        if (full) {
          break;
        }
      }
    }

    // Handle the excess replicas in the rule.
    if (excessReplicas.size() < delCandidates.size() - expectedNumOfReplicas) {
      // Remove the excess replicas for every datacenter.
      for (ReplicationRuleSection section: rule.getSections()) {
        String dcName = section.getDataCenter();
        if (dcMap.get(dcName) != null
            && dcMap.get(dcName).size() > section.getReplica()) {
          List<DatanodeStorageInfo> dcStorageInfos = dcMap.get(dcName);
          List <DatanodeStorageInfo> dcExcessReplicas =
              super.chooseReplicasToDelete(dcStorageInfos, dcStorageInfos,
                  section.getReplica(), excessTypes, addedNode, delNodeHint);
          boolean full = addExcessReplicas(delCandidates, expectedNumOfReplicas,
              dcExcessReplicas, excessReplicas, delNodeHint);
          if (full) {
            break;
          }
        }
      }
    }
    return excessReplicas;
  }

  @Override
  public List<DatanodeStorageInfo> chooseReplicasToDelete(
      Collection<DatanodeStorageInfo> availableReplicas,
      Collection<DatanodeStorageInfo> delCandidates,
      int expectedNumOfReplicas,
      List<StorageType> excessTypes,
      DatanodeDescriptor addedNode,
      DatanodeDescriptor delNodeHint,
      Collection<String> delRedundantDataCenters) {

    final Map<String, List<DatanodeStorageInfo>> dcMap = new HashMap<>();
    splitNodesWithDataCenter(delCandidates, dcMap);
    // Check if there is only one data center.
    if (dcMap.size() == 1) {
      return super.chooseReplicasToDelete(availableReplicas, delCandidates,
          expectedNumOfReplicas, excessTypes, addedNode, delNodeHint);
    }

    List<DatanodeStorageInfo> excessReplicas = new ArrayList<>();

    boolean needMoreExcessReplicas = true;
    // Add excess replicas from the specified data center.
    if (delRedundantDataCenters != null && !delRedundantDataCenters.isEmpty()) {
      for (String delDataCenter : delRedundantDataCenters) {
        if (dcMap.containsKey(delDataCenter)) {
          List<DatanodeStorageInfo> dcStorageInfos = dcMap.get(delDataCenter);
          needMoreExcessReplicas = !addExcessReplicas(delCandidates, expectedNumOfReplicas,
              dcStorageInfos, excessReplicas, null);
          if (!needMoreExcessReplicas) {
            break;
          }
        }
      }
    }

    if (needMoreExcessReplicas) {
      // If needMoreExcessReplicas is true, choose replicas to delete from remaining candidates.
      Collection<DatanodeStorageInfo> copyDelCandidates = new ArrayList<>(delCandidates);
      copyDelCandidates.removeIf(excessReplicas::contains);
      List <DatanodeStorageInfo> otherExcessReplicas =
          super.chooseReplicasToDelete(copyDelCandidates, copyDelCandidates,
              expectedNumOfReplicas, excessTypes, addedNode, null);
      excessReplicas.addAll(otherExcessReplicas);
    }

    return excessReplicas;
  }
  /**
   * Add replicas to excessReplicas list.
   */
  private boolean addExcessReplicas(
      Collection<DatanodeStorageInfo> candidates,
      int expectedNumOfReplicas,
      List <DatanodeStorageInfo> toAddReplicas,
      List<DatanodeStorageInfo> excessReplicas,
      DatanodeDescriptor delNodeHint) {
    int left = candidates.size() - expectedNumOfReplicas
        - excessReplicas.size();
    if (toAddReplicas.size() > left) {
      // Add delHint to excessReplicas if it exists in toAddReplicas.
      Iterator<DatanodeStorageInfo> it = toAddReplicas.iterator();
      while (delNodeHint != null && it.hasNext()) {
        DatanodeStorageInfo storageInfo = it.next();
        if (storageInfo.getDatanodeDescriptor().equals(delNodeHint)) {
          excessReplicas.add(storageInfo);
          toAddReplicas.remove(storageInfo);
          left = left - 1;
          break;
        }
      }
      for (int i = 0; i < left; ++i) {
        excessReplicas.add(toAddReplicas.get(i));
      }
    } else {
      excessReplicas.addAll(toAddReplicas);
    }
    return candidates.size() - expectedNumOfReplicas == excessReplicas.size();
  }

  /**
   * Split data nodes into datacenter sets.
   *
   * @param storageInfos DatanodeStorageInfo to be split
   * @param dcMap a map from datacenter to datanodes
   */
  public void splitNodesWithDataCenter(
      final Collection<DatanodeStorageInfo> storageInfos,
      final Map<String, List<DatanodeStorageInfo>> dcMap) {
    for (DatanodeStorageInfo s : storageInfos) {
      final String dcName = NetworkTopologyUtil.getDataCenter(
          s.getDatanodeDescriptor());
      List<DatanodeStorageInfo> storageList = dcMap.computeIfAbsent(
          dcName, k -> new ArrayList<>());
      storageList.add(s);
    }
  }

  @Override
  public DatanodeStorageInfo[] chooseTarget(
      String srcPath, int numOfReplicas,
      ReplicationRule rule, Node writer,
      List<DatanodeStorageInfo> chosenNodes,
      boolean returnChosenNodes, Set<Node> excludedNodes,
      long blocksize, final BlockStoragePolicy storagePolicy,
      EnumSet<AddBlockFlag> flags) {
    final Map<String, List<DatanodeStorageInfo>> dcMap = new HashMap<>();
    splitNodesWithDataCenter(chosenNodes, dcMap);

    // Replicate to the datacenter without any replicas first
    Set<String> exists = dcMap.keySet();
    for (ReplicationRuleSection section: rule.getSections()) {
      String dc = section.getDataCenter();
      if (section.getReplica() > 0 && !exists.contains(dc)) {
        Node base = dcBaseNodes.get(dc);
        if (base == null) {
          base = new NodeBase(VIRTUAL_HOST, dc + VIRTUAL_RACK);
          dcBaseNodes.put(dc, base);
        }
        int n = Math.min(numOfReplicas, section.getReplica());
        // do not put datanodes in multiple datacenters to one pipeline,
        // so return immediately
        return super.chooseTarget(srcPath, n, base, EMPTY_NODES,
            returnChosenNodes, excludedNodes, blocksize,
            storagePolicy, flags);
      }
    }

    // Replicate to datacenters without enough replicas
    for (ReplicationRuleSection section: rule.getSections()) {
      if (section.getReplica() <= 0) {
        continue;
      }
      List<DatanodeStorageInfo> storages = dcMap.get(section.getDataCenter());
      if ((storages != null) && (section.getReplica() > storages.size())) {
        int n = Math.min(numOfReplicas, section.getReplica() - storages.size());
        Node base = (writer != null && NetworkTopologyUtil.getDataCenter(writer)
            .equals(section.getDataCenter())) ?
            writer: storages.get(0).getDatanodeDescriptor();
        return super.chooseTarget(srcPath, n, base,
            NetworkTopologyUtil.getStoragesInDataCenter(chosenNodes,
                section.getDataCenter()), returnChosenNodes,
            excludedNodes, blocksize, storagePolicy, flags);
      }
    }

    return DatanodeStorageInfo.EMPTY_ARRAY;
  }
}
