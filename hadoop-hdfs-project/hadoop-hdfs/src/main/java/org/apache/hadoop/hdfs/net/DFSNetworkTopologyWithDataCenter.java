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
package org.apache.hadoop.hdfs.net;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * The HDFS specific network topology class which is data center awareness.
 * This class extends DFSNetworkTopology to represents a cluster of nodes
 * with a 4-layers hierarchical network topology:
 * node-local, rack-local, datacenter-local and datacenter-off.
 * In this network topology, leaves represent data nodes (computers) and inner
 * nodes represent switches/routers that manage traffic in/out of data centers,
 * racks.
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
@InterfaceStability.Unstable
public class DFSNetworkTopologyWithDataCenter extends DFSNetworkTopology{
  private static final Logger LOG =
      LoggerFactory.getLogger(DFSNetworkTopologyWithDataCenter.class);

  public final static String DEFAULT_DATACENTER = "/default-datacenter";
  private final static String PATH_SEPARATOR = "/";
  private final static String ROOT_PATH = "/";

  public static DFSNetworkTopologyWithDataCenter getInstance(
      Configuration conf) {
    DFSNetworkTopologyWithDataCenter nt = ReflectionUtils.newInstance(
        conf.getClass(
            DFSConfigKeys.DFS_NET_TOPOLOGY_IMPL_KEY,
            DFSNetworkTopologyWithDataCenter.class,
            DFSNetworkTopologyWithDataCenter.class), conf);
    return (DFSNetworkTopologyWithDataCenter) nt.init(
        DFSTopologyNodeImpl.FACTORY);
  }

  /**
   * Get data center from a given string representation of a rack.
   * @param loc a path-like string representation of a network location.
   * @return a data center string
   */
  public static String getDataCenter(String loc) {
    loc = NodeBase.normalize(loc);
    String[] loc_info = loc.substring(1).split(PATH_SEPARATOR);
    if (loc_info.length == 1) {
      return DEFAULT_DATACENTER;
    } else if (loc_info.length == 2) {
      return ROOT_PATH + loc_info[0];
    } else {
      LOG.warn("Invalid node location with data center: {}.", loc);
      return DEFAULT_DATACENTER;
    }
  }

  /**
   * Choose a random node based on given scope and excludedNodes set.
   *
   * For scope, it will handle '~' with Data Centre:
   * 1. If scope like "~/DC1/rack1". This function will choose the node which is
   *    in DC1 but not in rack1. As we do not support datacenter-off placement.
   *    A client can only write to DataNodes in the same DC.
   * 2. If scope like "~/DC1". This function will choose the node which is
   *    not in DC1. This case is not expected to occur.
   * 3. If scope doesn't contain '~', This function will choose the node
   *    in the scope.
   *
   * @param scope the scope where we look for node.
   * @param excludedNodes the returned node must not be in this set
   * @return a node with required storage type
   */
  @Override
  public Node chooseRandom(final String scope,
      final Collection<Node> excludedNodes) {
    netlock.readLock().lock();
    try {
      if (scope.startsWith("~")) {
        if (scope.substring(2).split(PATH_SEPARATOR).length > 1) {
          return chooseRandom(getDataCenter(scope.substring(1)),
              scope.substring(1), excludedNodes);
        } else {
          return chooseRandom(NodeBase.ROOT, scope.substring(1),
              excludedNodes);
        }
      } else {
        return chooseRandom(scope, null, excludedNodes);
      }
    } finally {
      netlock.readLock().unlock();
    }
  }

  /**
   * Randomly choose one node from <i>scope</i>, with specified storage type.
   *
   * For scope, it will handle '~' with Data Centre:
   * 1. If scope like "~/DC1/rack1". This function will choose the node which is
   *    in DC1 but not in rack1. As we do not support datacenter-off placement.
   *    A client can only write to DataNodes in the same DC.
   * 2. If scope like "~/DC1". This function will choose the node which is
   *    not in DC1. This case is not expected to occur.
   * 3. If scope doesn't contain '~', This function will choose the node
   *    in the scope.
   *
   * @param scope range of nodes from which a node will be chosen
   * @param excludedNodes nodes to be excluded from
   * @param type the storage type we search for
   * @return the chosen node
   */
  public Node chooseRandomWithStorageType(final String scope,
      final Collection<Node> excludedNodes, StorageType type) {
    netlock.readLock().lock();
    try {
      if (scope.startsWith("~")) {
        if (scope.substring(2).split(PATH_SEPARATOR).length > 1) {
          return chooseRandomWithStorageType(
              getDataCenter(scope.substring(1)),
              scope.substring(1), excludedNodes, type);
        } else {
          return chooseRandomWithStorageType(NodeBase.ROOT,
              scope.substring(1), excludedNodes, type);
        }
      } else {
        return chooseRandomWithStorageType(
            scope, null, excludedNodes, type);
      }
    } finally {
      netlock.readLock().unlock();
    }
  }

  /**
   * Randomly choose one node from <i>scope</i> with the given storage type.
   *
   * For scope, it will handle '~' with Data Centre:
   * 1. If scope like "~/DC1/rack1". This function will choose the node which is
   *    in DC1 but not in rack1. As we do not support datacenter-off placement.
   *    A client can only write to DataNodes in the same DC.
   * 2. If scope like "~/DC1". This function will choose the node which is
   *    not in DC1. This case is not expected to occur.
   * 3. If scope doesn't contain '~', This function will choose the node
   *    in the scope.
   *
   * This call would make up to two calls. It first tries to get a random node
   * (with old method) and check if it satisfies. If yes, simply return it.
   * Otherwise, it make a second call (with the new method) by passing in a
   * storage type.
   *
   * This is for better performance reason. Put in short, the key note is that
   * the old method is faster but may take several runs, while the new method
   * is somewhat slower, and always succeed in one trial.
   * See HDFS-11535 for more detail.
   *
   * @param scope range of nodes from which a node will be chosen
   * @param excludedNodes nodes to be excluded from
   * @param type the storage type we search for
   * @return the chosen node
   */
  @Override
  public Node chooseRandomWithStorageTypeTwoTrial(final String scope,
      final Collection<Node> excludedNodes, StorageType type) {
    netlock.readLock().lock();
    try {
      String searchScope;
      String excludedScope;
      if (scope.startsWith("~")) {
        if (scope.substring(2).split(PATH_SEPARATOR).length > 1) {
          searchScope = getDataCenter(scope.substring(1));
        } else {
          searchScope = NodeBase.ROOT;
        }
        excludedScope = scope.substring(1);
      } else {
        searchScope = scope;
        excludedScope = null;
      }
      // next do a two-trial search
      // first trial, call the old method, inherited from NetworkTopology
      Node n = chooseRandom(searchScope, excludedScope, excludedNodes);
      if (n == null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("No node to choose.");
        }
        // this means there is simply no node to choose from
        return null;
      }
      Preconditions.checkArgument(n instanceof DatanodeDescriptor);
      DatanodeDescriptor dnDescriptor = (DatanodeDescriptor)n;

      if (dnDescriptor.hasStorageType(type)) {
        // the first trial succeeded, just return
        return dnDescriptor;
      } else {
        // otherwise, make the second trial by calling the new method
        LOG.debug("First trial failed, node has no type {}, " +
            "making second trial carrying this type", type);
        return chooseRandomWithStorageType(searchScope, excludedScope,
            excludedNodes, type);
      }
    } finally {
      netlock.readLock().unlock();
    }
  }
}