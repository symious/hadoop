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
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.hdfs.net.NetworkTopologyUtil;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.zoneservice.ReplicationRule;
import org.apache.hadoop.hdfs.server.zoneservice.ReplicationRuleSection;
import org.apache.hadoop.hdfs.server.zoneservice.ReplicationRuleUtil;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BLOCK_PLACEMENT_POLICY_WITH_DATA_CENTER_FALLBACK_DC_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BLOCK_PLACEMENT_POLICY_WITH_DATA_CENTER_FALLBACK_DC_KEY;

/**
 * The class is responsible for choosing the desired number of targets
 * for placing block replicas on an environment with datacenter awareness.
 * The strategy is that it tries its best to place the replicas to most racks.
 */
public class BlockPlacementPolicyRackFaultTolerantDataCenter extends
    BlockPlacementPolicyWithDataCenter {

  private String defaultDC;
  private String defaultScope = null;

  @Override
  public void initialize(Configuration conf, FSClusterStats stats,
      NetworkTopology clusterMap, Host2NodesMap host2datanodeMap) {
    this.defaultDC = conf.get(
        DFS_NAMENODE_BLOCK_PLACEMENT_POLICY_WITH_DATA_CENTER_FALLBACK_DC_KEY,
        DFS_NAMENODE_BLOCK_PLACEMENT_POLICY_WITH_DATA_CENTER_FALLBACK_DC_DEFAULT);
    this.defaultScope = this.defaultDC == null ? null : "/" + this.defaultDC;
    if (this.defaultScope == null) {
      throw new IllegalArgumentException("The default scope shouldn't be null!");
    }
    super.initialize(conf, stats, clusterMap, host2datanodeMap);
  }

  /**
   * Refer to {@link BlockPlacementPolicyRackFaultTolerant#getMaxNodesPerRack}
   * when making changes.
   */
  @Override
  protected int[] getMaxNodesPerRack(int numOfChosen, int numOfReplicas) {
    int clusterSize = clusterMap.getNumOfLeaves();
    int totalNumOfReplicas = numOfChosen + numOfReplicas;
    if (totalNumOfReplicas > clusterSize) {
      numOfReplicas -= (totalNumOfReplicas-clusterSize);
      totalNumOfReplicas = clusterSize;
    }
    // No calculation needed when there is only one rack or picking one node.
    int numOfRacks = clusterMap.getNumOfNonEmptyRacks();
    // HDFS-14527 return default when numOfRacks = 0 to avoid
    // ArithmeticException when calc maxNodesPerRack at following logic.
    if (numOfRacks <= 1 || totalNumOfReplicas <= 1) {
      return new int[] {numOfReplicas, totalNumOfReplicas};
    }
    // If more racks than replicas, put one replica per rack.
    if (totalNumOfReplicas < numOfRacks) {
      return new int[] {numOfReplicas, 1};
    }
    // If more replicas than racks, evenly spread the replicas.
    // This calculation rounds up.
    int maxNodesPerRack = (totalNumOfReplicas - 1) / numOfRacks + 1;
    return new int[] {numOfReplicas, maxNodesPerRack};
  }

  /**
   * Calculate the maximum number of replicas to allocate per rack based
   * on the specified data center.
   * @param dataCenter The data center for which to calculate the maximum nodes per rack.
   * @param numOfChosen The number of already chosen nodes.
   * @param numOfReplicas The number of additional nodes to allocate.
   * @return
   */
  private int getMaxNodesPerRackWithDataCenter(String dataCenter,
      int numOfChosen, int numOfReplicas) {
    // Obtain the total number of nodes in the specified data center.
    int clusterSize = clusterMap.getDataCenterNodes().getOrDefault(dataCenter, 0);
    int totalNumOfReplicas = numOfChosen + numOfReplicas;
    if (totalNumOfReplicas > clusterSize) {
      totalNumOfReplicas = clusterSize;
    }
    // No calculation needed when there is only one rack or picking one node.
    int numOfRacks = clusterMap.getDataCenterRacks().getOrDefault(dataCenter, 0);
    // HDFS-14527 return default when numOfRacks = 0 to avoid
    // ArithmeticException when calc maxNodesPerRack at following logic.
    if (numOfRacks <= 1 || totalNumOfReplicas <= 1) {
      return totalNumOfReplicas;
    }
    // If more racks than replicas, put one replica per rack.
    if (totalNumOfReplicas < numOfRacks) {
      return 1;
    }
    // If more replicas than racks, evenly spread the replicas.
    // This calculation rounds up.
    return (totalNumOfReplicas - 1) / numOfRacks + 1;
  }

  @Override
  public DatanodeStorageInfo[] chooseTarget(
      String srcPath, int numOfReplicas,
      ReplicationRule rule, Node writer,
      List<DatanodeStorageInfo> chosenNodes,
      boolean returnChosenNodes, Set<Node> excludedNodes,
      long blocksize, final BlockStoragePolicy storagePolicy,
      EnumSet<AddBlockFlag> flags, boolean notEnoughRack) {

    if (notEnoughRack) {
      return chooseTarget(srcPath, numOfReplicas, writer,
          chosenNodes, returnChosenNodes, excludedNodes, blocksize, storagePolicy, flags);
    }

    final Map<String, List<DatanodeStorageInfo>> dcMap = new HashMap<>();
    ReplicationRuleUtil.splitNodesWithDataCenter(chosenNodes, dcMap);

    // Replicate to datacenters without enough replicas.
    List<DatanodeStorageInfo> targetNodes = new ArrayList<>();
    for (ReplicationRuleSection section: rule.getSections()) {
      if (section.getReplica() <= 0) {
        continue;
      }
      String dc = section.getDataCenter();
      List<DatanodeStorageInfo> storages = dcMap.getOrDefault(dc, new ArrayList<>());
      if (section.getReplica() > storages.size()) {
        int n = Math.min(numOfReplicas, section.getReplica() - storages.size());
        Node base = (writer != null && NetworkTopologyUtil.getDataCenter(writer)
            .equals(dc)) ? writer : new NodeBase(VIRTUAL_HOST, dc + VIRTUAL_RACK);
        DatanodeStorageInfo[] result =  super.chooseTarget(srcPath, n, base,
            NetworkTopologyUtil.getStoragesInDataCenter(chosenNodes,
                section.getDataCenter()), returnChosenNodes,
            excludedNodes, blocksize, storagePolicy, flags);
        Collections.addAll(targetNodes, result);
        if (result.length == numOfReplicas) {
          return targetNodes.toArray(DatanodeStorageInfo.EMPTY_ARRAY);
        }
        Collections.addAll(storages, result);
        numOfReplicas = Math.max(numOfReplicas - result.length, 0);
      }
    }

    return DatanodeStorageInfo.EMPTY_ARRAY;
  }

  /**
   * Refer to {@link BlockPlacementPolicyRackFaultTolerant#chooseTargetInOrder}
   * when making changes.
   */
  @Override
  protected Node chooseTargetInOrder(int numOfReplicas,
      Node writer,
      final Set<Node> excludedNodes,
      final long blocksize,
      final int maxNodesPerRack,
      final List<DatanodeStorageInfo> results,
      final boolean avoidStaleNodes,
      final boolean newBlock,
      EnumMap<StorageType, Integer> storageTypes)
      throws NotEnoughReplicasException {
    try {
      return chooseTarget(numOfReplicas, writer, excludedNodes,
          blocksize, maxNodesPerRack, results, avoidStaleNodes, newBlock, storageTypes);
    } catch (NotEnoughReplicasException e) {
      // Fallback case for no nodes found
      if (results.isEmpty() && this.defaultScope != null) {
        LOG.debug("Failed to choose any DNs for writer {}, falling back to {}.",
            writer.getNetworkLocation(), this.defaultDC);
        writer = chooseRandom(1, this.defaultScope, excludedNodes,
            blocksize, maxNodesPerRack, results, avoidStaleNodes, storageTypes).
            getDatanodeDescriptor();
        if (--numOfReplicas == 0) {
          return writer;
        }
        chooseTarget(numOfReplicas, writer, excludedNodes,
            blocksize, maxNodesPerRack, results, avoidStaleNodes, false, storageTypes);
        return writer;
      }
      // Fallback case for pipeline rebuild
      if (!results.isEmpty() && this.defaultScope != null) {
        boolean allInDefaultDC = true;
        for (DatanodeStorageInfo dsi : results) {
          if (!dsi.getDatanodeDescriptor().getNetworkLocation().startsWith(this.defaultScope)) {
            allInDefaultDC = false;
            break;
          }
        }
        if (allInDefaultDC) {
          LOG.debug("All chosen nodes are from default DC " + this.defaultDC
              + ", choosing more nodes from default DC for writer "
              + writer.getNetworkLocation());
          chooseTarget(numOfReplicas, results.get(0).getDatanodeDescriptor(), excludedNodes,
              blocksize, maxNodesPerRack, results, avoidStaleNodes, false, storageTypes);
          return writer;
        }
      }
      // Rethrow if cannot fallback to default DC.
      throw e;
    }
  }

  /**
   * Choose numOfReplicas in order:
   * 1. If total replica expected is less than numOfRacks in cluster, it choose
   * randomly.
   * 2. If total replica expected is bigger than numOfRacks, it choose:
   *  2a. Fill each rack exactly (maxNodesPerRack-1) replicas.
   *  2b. For some random racks, place one more replica to each one of them,
   *  until numOfReplicas have been chosen. <br>
   * 3. If after step 2, there are still replicas not placed (due to some
   * racks have fewer datanodes than maxNodesPerRack), the rest of the replicas
   * is placed evenly on the rest of the racks who have Datanodes that have
   * not been placed a replica.
   * 4. If after step 3, there are still replicas not placed. A
   * {@link NotEnoughReplicasException} is thrown.
   * <p>
   * For normal setups, step 2 would suffice. So in the end, the difference
   * of the numbers of replicas for each two racks is no more than 1.
   * Either way it always prefer local storage.
   * @return local node of writer
   */
  protected Node chooseTarget(int numOfReplicas,
      Node writer,
      final Set<Node> excludedNodes,
      final long blocksize,
      int maxNodesPerRack,
      final List<DatanodeStorageInfo> results,
      final boolean avoidStaleNodes,
      final boolean newBlock,
      EnumMap<StorageType, Integer> storageTypes)
      throws NotEnoughReplicasException {
    int totalReplicaExpected = results.size() + numOfReplicas;

    try {
      // Determine the data center base on the writer.
      if (newBlock) {
        writer = chooseOnce(1, writer, excludedNodes, blocksize,
            maxNodesPerRack, results, avoidStaleNodes, storageTypes);
        if (--numOfReplicas == 0) {
          return writer;
        }
      } else {
        if (!NetworkTopologyUtil.compareDataCenters(writer,
            results.get(0).getDatanodeDescriptor())) {
          writer = results.get(0).getDatanodeDescriptor();
        }
      }

      // Get the data center of the chosen writer node.
      String dataCenter = NetworkTopologyUtil.getDataCenter(writer);
      // Calculate the maximum number of nodes per rack based on the data center.
      maxNodesPerRack = getMaxNodesPerRackWithDataCenter(
          dataCenter, results.size(), numOfReplicas);
      int numOfRacks = clusterMap.getNumOfNonEmptyRacks(dataCenter);
      if (totalReplicaExpected < numOfRacks ||
          totalReplicaExpected % numOfRacks == 0) {
        writer = chooseOnce(numOfReplicas, writer, excludedNodes, blocksize,
            maxNodesPerRack, results, avoidStaleNodes, storageTypes);
        return writer;
      }

      assert totalReplicaExpected > (maxNodesPerRack -1) * numOfRacks;

      // Calculate numOfReplicas for filling each rack exactly (maxNodesPerRack-1)
      // replicas.
      HashMap<String, Integer> rackCounts = new HashMap<>();
      for (DatanodeStorageInfo dsInfo : results) {
        String rack = dsInfo.getDatanodeDescriptor().getNetworkLocation();
        Integer count = rackCounts.get(rack);
        if (count != null) {
          rackCounts.put(rack, count + 1);
        } else {
          rackCounts.put(rack, 1);
        }
      }
      int excess = 0; // Sum of the above (maxNodesPerRack-1) part of nodes in results
      for (int count : rackCounts.values()) {
        if (count > maxNodesPerRack -1) {
          excess += count - (maxNodesPerRack -1);
        }
      }
      numOfReplicas = Math.min(totalReplicaExpected - results.size(),
          (maxNodesPerRack -1) * numOfRacks - (results.size() - excess));
      // Try to spread the replicas as evenly as possible across racks.
      // This is done by first placing with (maxNodesPerRack-1), then spreading
      // the remainder by calling again with maxNodesPerRack.
      writer = chooseOnce(numOfReplicas, writer, new HashSet<>(excludedNodes),
          blocksize, maxNodesPerRack - 1, results, avoidStaleNodes,
          storageTypes);

      // Exclude the chosen nodes
      for (DatanodeStorageInfo resultStorage : results) {
        addToExcludedNodes(resultStorage.getDatanodeDescriptor(),
            excludedNodes);
      }
      LOG.trace("Chosen nodes: {}", results);
      LOG.trace("Excluded nodes: {}", excludedNodes);

      numOfReplicas = totalReplicaExpected - results.size();
      chooseOnce(numOfReplicas, writer, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    } catch (NotEnoughReplicasException e) {
      LOG.warn("Only able to place {} of total expected {}"
              + " (maxNodesPerRack={}, numOfReplicas={}) nodes "
              + "evenly across racks, falling back to evenly place on the "
              + "remaining racks. This may not guarantee rack-level fault "
              + "tolerance. Please check if the racks are configured properly.",
          results.size(), totalReplicaExpected, maxNodesPerRack, numOfReplicas);
      LOG.debug("Caught exception was:", e);
      chooseEvenlyFromRemainingRacks(writer, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes,
          totalReplicaExpected, e);

    }

    return writer;
  }

  /**
   * Choose as evenly as possible from the racks which have available datanodes.
   */
  private void chooseEvenlyFromRemainingRacks(Node writer,
      Set<Node> excludedNodes, long blocksize, int maxNodesPerRack,
      List<DatanodeStorageInfo> results, boolean avoidStaleNodes,
      EnumMap<StorageType, Integer> storageTypes, int totalReplicaExpected,
      NotEnoughReplicasException e) throws NotEnoughReplicasException {
    int numResultsOflastChoose = 0;
    NotEnoughReplicasException lastException = e;
    int bestEffortMaxNodesPerRack = maxNodesPerRack;
    while (results.size() != totalReplicaExpected &&
        numResultsOflastChoose != results.size()) {
      // Exclude the chosen nodes
      final Set<Node> newExcludeNodes = new HashSet<>();
      for (DatanodeStorageInfo resultStorage : results) {
        addToExcludedNodes(resultStorage.getDatanodeDescriptor(),
            newExcludeNodes);
      }

      LOG.trace("Chosen nodes: {}", results);
      LOG.trace("Excluded nodes: {}", excludedNodes);
      LOG.trace("New Excluded nodes: {}", newExcludeNodes);
      final int numOfReplicas = totalReplicaExpected - results.size();
      numResultsOflastChoose = results.size();
      try {
        chooseOnce(numOfReplicas, writer, newExcludeNodes, blocksize,
            ++bestEffortMaxNodesPerRack, results, avoidStaleNodes,
            storageTypes);
      } catch (NotEnoughReplicasException nere) {
        lastException = nere;
      } finally {
        excludedNodes.addAll(newExcludeNodes);
      }
    }

    if (numResultsOflastChoose != totalReplicaExpected) {
      LOG.debug("Best effort placement failed: expecting {} replicas, only "
          + "chose {}.", totalReplicaExpected, numResultsOflastChoose);
      throw lastException;
    }
  }

  /**
   * Randomly choose <i>numOfReplicas</i> targets from the given <i>scope</i>.
   * Except that 1st replica prefer local storage.
   * @return local node of writer.
   */
  private Node chooseOnce(int numOfReplicas,
      Node writer,
      final Set<Node> excludedNodes,
      final long blocksize,
      final int maxNodesPerRack,
      final List<DatanodeStorageInfo> results,
      final boolean avoidStaleNodes,
      EnumMap<StorageType, Integer> storageTypes)
      throws NotEnoughReplicasException {
    if (numOfReplicas == 0) {
      return writer;
    }

    // results list is the target nodes already chosen and exclude DECOMMISSIONING nodes,
    // if node is DECOMMISSIONING and try a node on local rack, otherwise choose randomly,
    // Here is a point to explain, currently we only consider the scenario
    // where maxNodesPerRack is 1.
    final Node tmpWriter = writer;
    boolean isInResult = (tmpWriter != null && results.stream().anyMatch(datanodeStorageInfo ->
        datanodeStorageInfo.getDatanodeDescriptor().getName().equals(tmpWriter.getName())));
    if (!isInResult) {
      writer = chooseLocalStorage(writer, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes, true)
          .getDatanodeDescriptor();
      if (--numOfReplicas == 0) {
        return writer;
      }
    }

    DatanodeStorageInfo datanodeStorageInfo = chooseRandom(writer, numOfReplicas,
        NodeBase.ROOT, excludedNodes, blocksize, maxNodesPerRack,
        results, avoidStaleNodes, storageTypes);

    if (isInResult) {
      writer = datanodeStorageInfo.getDatanodeDescriptor();
    }

    return writer;
  }

  /**
   * Refer to {@link BlockPlacementPolicyRackFaultTolerant#verifyBlockPlacement}
   * when making changes.
   */
  @Override
  public BlockPlacementStatus verifyBlockPlacement(DatanodeInfo[] locs,
      int numberOfReplicas) {
    if (locs == null)
      locs = DatanodeDescriptor.EMPTY_ARRAY;
    if (!clusterMap.hasClusterEverBeenMultiRack()) {
      // only one rack
      return new BlockPlacementStatusDefault(1, 1, 1);
    }
    // Count locations on different racks.
    Set<String> racks = new HashSet<>();
    for (DatanodeInfo dn : locs) {
      racks.add(dn.getNetworkLocation());
    }
    String dataCenter = NetworkTopologyUtil.getDataCenter(locs[0].getNetworkLocation());
    return new BlockPlacementStatusDefault(racks.size(), numberOfReplicas,
        clusterMap.getNumOfNonEmptyRacks(dataCenter));
  }

  /**
   * Refer to {@link BlockPlacementPolicyRackFaultTolerant#pickupReplicaSet}
   * when making changes.
   */
  @Override
  protected Collection<DatanodeStorageInfo> pickupReplicaSet(
      Collection<DatanodeStorageInfo> moreThanOne,
      Collection<DatanodeStorageInfo> exactlyOne,
      Map<String, List<DatanodeStorageInfo>> rackMap) {
    return moreThanOne.isEmpty() ? exactlyOne : moreThanOne;
  }

  @VisibleForTesting
  public void setDefaultDC(String defaultDC) {
    this.defaultDC = defaultDC;
    this.defaultScope = "/" + defaultDC;
  }

  @Override
  public List<DatanodeStorageInfo> chooseReplicasToDelete(
      Collection<DatanodeStorageInfo> availableReplicas,
      Collection<DatanodeStorageInfo> delCandidates,
      int expectedNumOfReplicas, ReplicationRule rule,
      List<StorageType> excessTypes, DatanodeDescriptor addedNode,
      DatanodeDescriptor delNodeHint) {

    List<DatanodeStorageInfo> excessReplicas = new ArrayList<>();
    Map<String, List<DatanodeStorageInfo>> delDCMap = new HashMap<>();
    ReplicationRuleUtil.splitNodesWithDataCenter(delCandidates, delDCMap);
    // DelCandidates are in one datacenter.
    if (delDCMap.size() == 1) {
      return super.chooseReplicasToDelete(availableReplicas, delCandidates,
          expectedNumOfReplicas, excessTypes, addedNode, delNodeHint);
    }

    // Handle the replicas which are not included in the rule.
    for (Map.Entry<String, List<DatanodeStorageInfo>> entry : delDCMap.entrySet()) {
      String dcName = entry.getKey();
      List<DatanodeStorageInfo> storageInfos = entry.getValue();
      if (!rule.getDatacenters().contains(dcName)) {
        if (addExcessReplicas(delCandidates, expectedNumOfReplicas,
            storageInfos, excessReplicas, delNodeHint)) {
          break;
        }
      }
    }

    // Handle the excess replicas in the rule.
    if (excessReplicas.size() < delCandidates.size() - expectedNumOfReplicas) {
      Map<String, List<DatanodeStorageInfo>> availableDCMap = new HashMap<>();
      ReplicationRuleUtil.splitNodesWithDataCenter(availableReplicas, availableDCMap);
      for (DatanodeStorageInfo datanodeStorageInfo : delCandidates) {
        String dcName = NetworkTopologyUtil.getDataCenter(
            datanodeStorageInfo.getDatanodeDescriptor());
        List<DatanodeStorageInfo> availableDCStorageInfos = availableDCMap.get(dcName);
        List<DatanodeStorageInfo> delDCStorageInfos = delDCMap.get(dcName);
        if (availableDCStorageInfos.size() > rule.getReplica(dcName)) {
          List<DatanodeStorageInfo> replicasToDelete = (delDCStorageInfos.size() == 1) ?
              Stream.of(datanodeStorageInfo).collect(Collectors.toList()) :
              super.chooseReplicasToDelete(availableDCStorageInfos,
                  Stream.of(datanodeStorageInfo).collect(Collectors.toList()),
                  0, excessTypes, addedNode,
                  datanodeStorageInfo.getDatanodeDescriptor());
          if (addExcessReplicas(delCandidates, expectedNumOfReplicas,
              replicasToDelete, excessReplicas, delNodeHint)) {
            break;
          }
        }
      }
    }
    return excessReplicas;
  }
}
