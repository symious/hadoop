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
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.net.Node;

import java.util.ArrayList;
import java.util.List;

/**
 * Util class for network topology.
 */
public class NetworkTopologyUtil {

  /**
   * Get data center from a given string representation of a rack.
   * @param loc a path-like string representation of a network location.
   * @return a data center string
   */
  public static String getDataCenter(String loc) {
    Preconditions.checkNotNull(loc);
    return DFSNetworkTopologyWithDataCenter.getDataCenter(loc);
  }

  /**
   * Get data center from a Node.
   * @param node instance
   * @return a data center string
   */
  public static String getDataCenter(Node node) {
    Preconditions.checkNotNull(node);
    return getDataCenter(node.getNetworkLocation());
  }

  /**
   * Get nodes located in the datacenter.
   * @param nodes set
   * @param datacenter specified
   * @param <T> node type
   * @return nodes in the datacenter
   */
  public static <T extends Node> List<T> getNodesInDataCenter(
      List<T> nodes, String datacenter) {
    List<T> selectNodes = new ArrayList<>();
    for (T node: nodes) {
      if (getDataCenter(node).equals(datacenter)) {
        selectNodes.add(node);
      }
    }
    return selectNodes;
  }

  /**
   * Get storages located in the datacenter.
   * @param storages set
   * @param datacenter specified
   * @return storages in the datacenter
   */
  public static List<DatanodeStorageInfo> getStoragesInDataCenter(
      List<DatanodeStorageInfo> storages, String datacenter) {
    List<DatanodeStorageInfo> selectStorages = new ArrayList<>();
    for (DatanodeStorageInfo storage: storages) {
      if (getDataCenter(storage.getDatanodeDescriptor()).equals(datacenter)) {
        selectStorages.add(storage);
      }
    }
    return selectStorages;
  }
}