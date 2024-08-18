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
package org.apache.hadoop.hdfs.server.zoneservice;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.net.DFSNetworkTopologyWithDataCenter;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.net.NetUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ZoneUtil {
  protected static final String DC_SEPARATOR = ",";
  protected static final String ROOT = "/";

  static void checkDataCenterValues(Configuration conf, ReplicationRule rule,
      Map<String, ReplicationRule> pathRuleMap)
      throws IllegalArgumentException {
    Set<ReplicationRule> rules = new HashSet<>();
    if (rule != null) {
      rules.add(rule);
    }
    if (pathRuleMap != null) {
      rules.addAll(pathRuleMap.values());
    }
    checkDataCenterValues(conf, rules);
  }

  private static void checkDataCenterValues(Configuration conf,
      Collection<ReplicationRule> rules) throws IllegalArgumentException {
    // get valid datacenters from configuration
    String datacenters = conf.get(
        DFSConfigKeys.DFS_ZONEMOVER_VALID_DATACENTERS_KEY,
        DFSConfigKeys.DFS_ZONEMOVER_VALID_DATACENTERS_DEFAULT);
    Set<String> validDataCenters = new HashSet<>
        (Arrays.asList(datacenters.trim().split(DC_SEPARATOR)));
    validDataCenters.remove("");

    // check datacenters in rules
    for (ReplicationRule rule: rules) {
      for (ReplicationRuleSection section: rule.getSections()) {
        if (!validDataCenters.contains(section.getDataCenter())) {
          throw new IllegalArgumentException(
              section.getDataCenter() + " is NOT a valid DataCenter!");
        }
      }
    }
  }

  /**
   * Get the block distribution.
   * @return a mapping from DC to DN list.
   */
  public static Map<String, List<DatanodeInfo>> getBlockDistributionDNs(LocatedBlock lb) {
    Map<String, List<DatanodeInfo>> distribution = new HashMap<>();
    for (DatanodeInfo dn : lb.getLocations()) {
      String dc = DFSNetworkTopologyWithDataCenter.getDataCenter(
          dn.getNetworkLocation());
      if (distribution.containsKey(dc)) {
        distribution.get(dc).add(dn);
      } else {
        distribution.put(dc, new ArrayList<>(Collections.singletonList(dn)));
      }
    }
    return distribution;
  }

  /**
   * Check if all blocks in the list have the same datacenter distribution.
   */
  public static boolean areBlocksDistributionConsistent(List<LocatedBlock> blocks) {
    boolean consistent = true;
    Map<String, Short> distribution = null;
    for (LocatedBlock block: blocks) {
      if (distribution == null) {
        distribution = getBlockDistribution(block);
      } else {
        if (!distribution.equals(getBlockDistribution(block))) {
          consistent = false;
          break;
        }
      }
    }
    return consistent;
  }

  /**
   * Get the datacenter distribution of the block.
   */
  static Map<String, Short> getBlockDistribution(LocatedBlock block) {
    Map<String, Short> distribution = new HashMap<>();
    // Count replica in each datacenter
    DatanodeInfo[] infos = block.getLocations();
    for (DatanodeInfo info: infos) {
      String dc = DFSNetworkTopologyWithDataCenter.getDataCenter(
          info.getNetworkLocation());
      short value = distribution.getOrDefault(dc, (short) 0);
      distribution.put(dc, (short) (value + 1));
    }
    return distribution;
  }

  /**
   * Get {@link ZoneMoveItem} list for the block.
   */
  public static List<ZoneMoveItem> getZoneMoveItems(
      final LocatedBlock block, ReplicationRule rule) {
    // calculate sources and targets
    Map<String, Short> distribution = getBlockDistribution(block);
    return getZoneMoveItems(distribution, rule);
  }

  /**
   * Return true if this file need to be migrated.
   */
  public static boolean isFileNeedMove(
      LocatedBlocks locatedBlocks, ReplicationRule rule) {
    for (LocatedBlock block : locatedBlocks.getLocatedBlocks()) {
      if (!getZoneMoveItems(block, rule).isEmpty()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get the URI of the specified namespace
   */
  protected static URI getNamespaceUri(CommandLine line, Configuration conf)
      throws IllegalArgumentException {
    Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
    if (!line.hasOption("namespace")) {
      if (namenodes.size() > 1) {
        throw new IllegalArgumentException(
            "Namespace must be specified in a federation cluster!");
      } else {
        return namenodes.iterator().next();
      }
    }

    String namespace = line.getOptionValue("namespace");
    for (URI namenode: namenodes) {
      if (namenode.getAuthority().equals(namespace)) {
        return namenode;
      }
    }
    throw new IllegalArgumentException(
        "Cannot find the NameNode for namespace: " + namespace);
  }

  public static ZoneMoverHttpServer startHttpServer(final Configuration conf) throws
      IOException {
    ZoneMoverHttpServer httpServer = new ZoneMoverHttpServer(conf, getHttpServerBindAddress(conf));
    httpServer.start();
    return httpServer;
  }

  /**
   * HTTP server address for binding the endpoint. This method is
   * for use by the ZoneMover and its derivatives. It may return
   * a different address than the one that should be used by clients to
   * connect to the ZoneMover. See
   * {@link DFSConfigKeys#DFS_ZONEMOVER_HTTP_BIND_HOST_KEY}
   *
   * @param conf configuration of zone mover
   * @return return the http bind address of zone mover
   */
  private static InetSocketAddress getHttpServerBindAddress(Configuration conf) {
    InetSocketAddress bindAddress = getHttpAddress(conf);

    // If DFS_ZONEMOVER_HTTP_BIND_HOST_KEY exists then it overrides the
    // host name portion of DFS_ZONEMOVER_HTTP_ADDRESS_KEY.
    final String bindHost = conf.getTrimmed(DFSConfigKeys.DFS_ZONEMOVER_HTTP_BIND_HOST_KEY);
    if (bindHost != null && !bindHost.isEmpty()) {
      bindAddress = new InetSocketAddress(bindHost, bindAddress.getPort());
    }

    return bindAddress;
  }

  /** @return the ZoneMover HTTP address. */
  private static InetSocketAddress getHttpAddress(Configuration conf) {
    return NetUtils.createSocketAddr(
        conf.getTrimmed(DFSConfigKeys.DFS_ZONEMOVER_HTTP_ADDRESS_KEY,
            DFSConfigKeys.DFS_ZONEMOVER_HTTP_ADDRESS_DEFAULT));
  }

  public static List<ZoneMoveItem> getZoneMoveItems(
      final Map<String, Short> distribution, ReplicationRule rule) {
    // calculate sources and targets
    Map<String, Short> sources = new HashMap<>();
    Map<String, Short> targets = new HashMap<>();
    Set<String> dcs = new HashSet<>();
    Map<String, Short> ruleMap = rule.toMap();
    dcs.addAll(distribution.keySet());
    dcs.addAll(ruleMap.keySet());
    for (String dc: dcs) {
      short m = 0;
      if (distribution.containsKey(dc)) {
        m = distribution.get(dc);
      }
      short n = 0;
      if (ruleMap.containsKey(dc)) {
        n = ruleMap.get(dc);
      }
      if (m > n) {
        sources.put(dc, (short) (m - n));
      } else if (m < n) {
        targets.put(dc, (short) (n - m));
      }
    }

    // generate ZoneMoveItem
    List<ZoneMoveItem> items = new ArrayList<>();
    for (Map.Entry<String, Short> sourceEntry: sources.entrySet()) {
      String sdc = sourceEntry.getKey();
      short x = sourceEntry.getValue();
      for (String tdc: new HashSet<>(targets.keySet())) {
        short y = targets.get(tdc);
        if (x == y) {
          items.add(new ZoneMoveItem(sdc, tdc, x));
          targets.remove(tdc);
          break;
        } else if (x > y) {
          items.add(new ZoneMoveItem(sdc, tdc, y));
          targets.remove(tdc);
          x = (short) (x - y);
        } else {
          items.add(new ZoneMoveItem(sdc, tdc, x));
          targets.put(tdc, (short) (y - x));
          break;
        }
      }
    }
    return items;
  }

  public static List<Path> getPaths(Map<String, ReplicationRule> pathRuleMap) {
    Set<String> stringPaths = pathRuleMap.keySet();
    List<Path> paths = new ArrayList<>();
    for (String path: stringPaths) {
      if (!path.startsWith(ROOT)) {
        throw new IllegalArgumentException("Invalid path: " + path);
      }
      paths.add(new Path(path));
    }
    return paths;
  }
}
