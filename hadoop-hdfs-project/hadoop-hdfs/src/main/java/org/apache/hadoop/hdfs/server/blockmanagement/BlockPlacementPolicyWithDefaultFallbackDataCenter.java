/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.EnumMap;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BLOCK_PLACEMENT_POLICY_WITH_DATA_CENTER_FALLBACK_DC_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BLOCK_PLACEMENT_POLICY_WITH_DATA_CENTER_FALLBACK_DC_KEY;

/**
 * See {@link BlockPlacementPolicyWithDataCenter}.
 * This class is essentially the same but returns off DC nodes in a
 * configurable fallback DC in the case of no valid DNs from the same DC as
 * the client.
 */
@InterfaceAudience.Private
public class BlockPlacementPolicyWithDefaultFallbackDataCenter
    extends BlockPlacementPolicyWithDataCenter {

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

  @Override
  protected Node chooseTargetInOrder(int numOfReplicas, Node writer,
      final Set<Node> excludedNodes, final long blocksize,
      final int maxNodesPerRack, final List<DatanodeStorageInfo> results,
      final boolean avoidStaleNodes, final boolean newBlock,
      EnumMap<StorageType, Integer> storageTypes) throws NotEnoughReplicasException {
    try {
      return super.chooseTargetInOrder(numOfReplicas, writer, excludedNodes,
          blocksize, maxNodesPerRack, results, avoidStaleNodes, newBlock, storageTypes);
    } catch (NotEnoughReplicasException e) {
      // Fallback case for no nodes found
      if (results.isEmpty() && this.defaultScope != null) {
        LOG.debug("Failed to choose any DNs for writer {}, falling back to {}.",
            writer.getNetworkLocation(), this.defaultDC);
        chooseRandom(numOfReplicas, this.defaultScope, excludedNodes,
            blocksize, maxNodesPerRack, results, avoidStaleNodes, storageTypes);
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
          chooseRandom(numOfReplicas - results.size(), this.defaultScope,
              excludedNodes, blocksize, maxNodesPerRack, results,
              avoidStaleNodes, storageTypes);
          return writer;
        }
      }
      // Rethrow if cannot fallback to default DC.
      throw e;
    }
  }

  @VisibleForTesting
  public void setDefaultDC(String defaultDC) {
    this.defaultDC = defaultDC;
    this.defaultScope = "/" + defaultDC;
  }
}