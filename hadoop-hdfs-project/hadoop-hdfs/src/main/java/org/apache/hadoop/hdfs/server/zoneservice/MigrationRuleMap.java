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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ZONEMOVER_DISTRIBUTION_RULE_MAP_FILE_KEY;

public class MigrationRuleMap {
  private static final Logger LOG = LoggerFactory.getLogger(MigrationRuleMap.class);
  private static final Map<ReplicationRule, ReplicationRule> ruleMap = new HashMap<>();
  private static final Map<Short, ReplicationRule> ruleMapWithZS = new HashMap<>();

  public MigrationRuleMap(Configuration conf) throws IOException {
   loadRuleMap(conf);
   loadRuleMapWithZS(conf);
   LOG.info("ZoneMover migration will follow the following rule map:\n " +
       "Distribution and rule map:\n {} \n " +
       "Used by zone service rule map:\n {}", ruleMap, ruleMapWithZS);
  }

  /**
   * Load rule map from the given config file.
   */
  private void loadRuleMap(Configuration conf) throws IOException {
    String mapPath = conf.get(DFS_ZONEMOVER_DISTRIBUTION_RULE_MAP_FILE_KEY);
    if (mapPath == null || mapPath.isEmpty()) {
      LOG.warn("Empty rule map for config: {}", DFS_ZONEMOVER_DISTRIBUTION_RULE_MAP_FILE_KEY);
      return;
    }

    File file = new File(mapPath);
    if (!file.exists()) {
      LOG.warn("{} rule map file does not exist", mapPath);
      return;
    }

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(
        Files.newInputStream(new File(mapPath).toPath()), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        try {
          String[] mapPair = line.trim().split("\\s+");
          ruleMap.put(ReplicationRule.parseFromString(mapPair[0]),
              ReplicationRule.parseFromString(mapPair[1]));
        } catch (Exception e) {
          LOG.warn("Unable to process line: {} ", line);
        }
      }
    } catch (IOException e) {
      LOG.warn("Unable to process file: {}", mapPath, e);
      throw e;
    }
  }

  /**
   * Load rule map from the given config file, used by zone service monitor.
   */
  private void loadRuleMapWithZS(Configuration conf) {
    Collection<String> replicaRuleCollections = StringUtils.getTrimmedStringCollection(
        conf.get(DFSConfigKeys.DFS_ZONE_SUPPORT_MIGRATE_REPLICA_RULES_KEY), ";");
    for (String replica : replicaRuleCollections) {
      String[] keyValue = replica.split("=");
      if (keyValue.length == 2) {
        Short factor = Short.valueOf(keyValue[0].trim());
        ruleMapWithZS.put(factor, ReplicationRule.parseFromString(keyValue[1].trim()));
      }
    }
  }

  /**
   * Get the replication rule based on the current distribution, replication factor,
   * source data center, target data center, and whether the replication is decreasing.
   *
   * @param distribution Current replication rule distribution
   * @param replication Desired replication factor
   * @param sourceDC Source data center
   * @param targetDC Target data center
   * @param isDecrease Flag indicating if replication is decreasing
   * @return The new replication rule based on the given parameters
   */
  protected ReplicationRule getRuleFromDistribution(ReplicationRule distribution,
      short replication, String sourceDC, String targetDC, boolean isDecrease) {
    ReplicationRule rule = ruleMap.get(distribution);
    if (rule != null) {
      return rule;
    }

    Set<String> datacenters = distribution.getDatacenters();
    if (isDecrease && datacenters.contains(sourceDC)) {
      return handleDecrease(distribution, replication, sourceDC, targetDC, datacenters);
    } else if (!isDecrease && datacenters.contains(sourceDC) && replication > 3) {
      return handleIncrease(distribution, replication, sourceDC, targetDC, datacenters);
    }
    return null;
  }

  /**
   * Handle the decrease in replication by redistributing replicas from the source data center.
   *
   * @param distribution Current replication rule distribution
   * @param replication Desired replication factor
   * @param sourceDC Source data center
   * @param targetDC Target data center
   * @param datacenters Set of current data centers
   * @return The new replication rule after decreasing replication
   */
  private ReplicationRule handleDecrease(ReplicationRule distribution, short replication,
      String sourceDC, String targetDC, Set<String> datacenters) {
    Map<String, Short> newDistribution = new HashMap<>();

    // If there is only one data center, move all replicas to the target data center,
    // such as /sourceDC:5 => /targetDC:5.
    if (datacenters.size() == 1) {
      newDistribution.put(targetDC, distribution.getReplica(sourceDC));
      return ReplicationRule.parseFromMap(newDistribution);
    }

    // If the desired replication factor is greater than 3, redistribute replicas accordingly.
    //   1.if there are two data centers, the purpose here is to migrate all replicas from the
    //   source data center, so it doesn't matter if the other data center is the target data center,
    //   such as /sourceDC:3,/targetDC:3 => /targetDC:6.
    //   2.if there are three data centers,
    //   first move all replicas from the source data center to the target data center,
    //   then retain the distribution for the remaining data center,
    //   such as /sourceDC:2,/targetDC:2,/otherDC:2 => /targetDC:4,/otherDC:2.
    if (replication > 3) {
      Set<String> remainingDatacenters = new HashSet<>(datacenters);
      if (datacenters.size() == 2) {
        remainingDatacenters.remove(sourceDC);
        String remainingDC = remainingDatacenters.iterator().next();
        newDistribution.put(remainingDC,
            (short) (distribution.getReplica(remainingDC) + distribution.getReplica(sourceDC)));
        return ReplicationRule.parseFromMap(newDistribution);
      }
      if (datacenters.size() == 3) {
        remainingDatacenters.remove(sourceDC);
        remainingDatacenters.remove(targetDC);
        String remainingDC = remainingDatacenters.iterator().next();
        newDistribution.put(targetDC,
            (short) (distribution.getReplica(targetDC) + distribution.getReplica(sourceDC)));
        newDistribution.put(remainingDC, distribution.getReplica(remainingDC));
        return ReplicationRule.parseFromMap(newDistribution);
      }
    }
    return null;
  }

  /**
   * Handle the increase in replication by redistributing replicas between the source and
   * target data centers only for replication factor greater than 3.
   *
   * @param distribution Current replication rule distribution
   * @param replication Desired replication factor
   * @param sourceDC Source data center
   * @param targetDC Target data center
   * @param datacenters Set of current data centers
   * @return The new replication rule after increasing replication
   */
  private ReplicationRule handleIncrease(ReplicationRule distribution, short replication,
      String sourceDC, String targetDC, Set<String> datacenters) {
    Map<String, Short> newDistribution = new HashMap<>();
    int replicasPerIDC; // Replicas per individual data center.
    int remainingReplicas; // Remaining replicas after division.
    // the source data center's replica count will set `replicasPerIDC` and
    // the target data center's replica count will set `replicasPerIDC + remainingReplicas`.

    // If there is only one data center, divide the replicas between source and target data centers.
    // such as /sourceDC:5 => /targetDC:3,/sourceDC:2.
    if (datacenters.size() == 1) {
      replicasPerIDC = replication >> 1;
      remainingReplicas = replication & 1;
      newDistribution.put(sourceDC, (short) replicasPerIDC);
      newDistribution.put(targetDC, (short) (replicasPerIDC + remainingReplicas));
      return ReplicationRule.parseFromMap(newDistribution);
    }

    short replica = distribution.getReplica(sourceDC);
    Set<String> remainingDatacenters = new HashSet<>(datacenters);
    // Determine replicas per IDC and remaining replicas based on source data center's
    // replica count, if source data center's replica count is 1,
    // set replicasPerIDC to 1 and remainingReplicas to 0,
    if (replica == 1) {
      replicasPerIDC = 1;
      remainingReplicas = 0;
    } else {
      replicasPerIDC = replica >> 1;
      remainingReplicas = replica & 1;
    }

    // If there are two data centers and should not contain target data center,
    // if source data center's replica count is 1, then to avoid upgrading the replica,
    // here one replica will be subtracted from another data center
    // (non-source data center and non-target data center).
    // such as /sourceDC:1,/otherDC:3 => /targetDC:1,/sourceDC:1,/otherDC:2
    // OR /sourceDC:2,/otherDC:3 => /targetDC:1,/sourceDC:1,/otherDC:3.
    if (datacenters.size() == 2 && !datacenters.contains(targetDC)) {
      remainingDatacenters.remove(sourceDC);
      newDistribution.put(sourceDC, (short) replicasPerIDC);
      newDistribution.put(targetDC, (short) (replicasPerIDC + remainingReplicas));
      String remainingDC = remainingDatacenters.iterator().next();
      newDistribution.put(remainingDC, replica == 1 ?
          (short) (distribution.getReplica(remainingDC) - 1) :
          distribution.getReplica(remainingDC));
      return ReplicationRule.parseFromMap(newDistribution);
    }

    // If there are three data center and should contain source data center and target data center
    // and the source data center's replica count must be greater than 1.
    // such as /sourceDC:2,/targetDC:2,/otherDC:1 => /targetDC:3,/sourceDC:1,/otherDC:1.
    if (datacenters.size() == 3 & replica > 1) {
      remainingDatacenters.remove(sourceDC);
      remainingDatacenters.remove(targetDC);
      String remainingDC = remainingDatacenters.iterator().next();
      newDistribution.put(sourceDC, (short) replicasPerIDC);
      newDistribution.put(targetDC, (short) (replicasPerIDC + remainingReplicas +
          distribution.getReplica(targetDC)));
      newDistribution.put(remainingDC, distribution.getReplica(remainingDC));
      return ReplicationRule.parseFromMap(newDistribution);
    }
    return null;
  }

  /**
   * This method determines the replication rules based on the distribution of data centers
   * and the specified replication factor, used by zone service monitor.
   *
   * @param distribution Current replication rule distribution
   * @param replication Desired replication factor
   * @param validDataCenters   The set of data centers that are considered valid for replication.
   */
  protected ReplicationRule getRuleFromDistributionWithZS(ReplicationRule distribution,
      short replication, Set<String> validDataCenters) {
    // Here validDataCenters will set 3 IDC [/AirTrunk,/YTL,/STT], if not will skip.
    if (validDataCenters.size() != 3) {
      return null;
    }

    Set<String> datacenters = distribution.getDatacenters();
    // Check if datacenters is a subset of validDataCenters or
    // if datacenters and validDataCenters are the same will return null.
    if (!validDataCenters.containsAll(datacenters) || datacenters.equals(validDataCenters)) {
      return null;
    }

    // Here if file is 2/3 replication will get rule from replicationRuleMap.
    // 2 replica =/AirTrunk:1,/YTL1:/STT:1 and the number of replicas will increase.
    // 3 replica =/AirTrunk:1,/YTL:2,/STT:1 and the number of replicas will increase.
    ReplicationRule rule = ruleMapWithZS.get(replication);
    if (rule != null) {
      return rule;
    }

    // > 3 replica = the replicas are randomly distributed among three IDC.
    if (replication > 3) {
      Map<String, Short> newDistribution = new HashMap<>();
      Set<String> needCenters = new HashSet<>(validDataCenters);
      needCenters.removeAll(datacenters);
      // Here the number of needCenters should be 1 or 2, each DC is set 1 replica.
      for (String dc : needCenters) {
        newDistribution.put(dc, (short) 1);
      }
      if (datacenters.size() == 1) {
        // The number of needCenters should be 2.
        String dc = datacenters.iterator().next();
        newDistribution.put(dc, (short) (replication - needCenters.size()));
        return ReplicationRule.parseFromMap(newDistribution);
      }
      if (datacenters.size() == 2) {
        // The number of needCenters should be 1.
        String primaryDC = distribution.getMainDataCenter();
        String secondaryDC = datacenters.stream()
            .filter(dc -> !dc.equals(primaryDC))
            .findFirst()
            .orElse(null);
        newDistribution.put(primaryDC, (short) (distribution.getReplica(primaryDC)
            - needCenters.size()));
        newDistribution.put(secondaryDC, distribution.getReplica(secondaryDC));
        return ReplicationRule.parseFromMap(newDistribution);
      }
    }
    return null;
  }
}