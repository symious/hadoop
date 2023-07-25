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
import org.apache.hadoop.hdfs.server.zoneservice.utils.MigrationDataCenters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ReplicaMigrationRuleMap {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicaMigrationRuleMap.class);
  private static final Map<ReplicationRule, ReplicationRule> ruleMap = new HashMap<>();
  private static final Map<ReplicationRule, ReplicationRule> degradeRuleMap = new HashMap<>();
  private static final Map<ReplicationRule, ReplicationRule> upgradeRuleMap = new HashMap<>();
  private static final Map<Short, ReplicationRule> defaultRuleMap = new HashMap<>();
  private static final Map<Integer, ReplicationRule> ruleAlias = new HashMap<>();

  public ReplicaMigrationRuleMap(Configuration conf) throws IOException {
    init();
    String mapPath = conf.get(DFSConfigKeys.DFS_ZONEMOVER_DISTRIBUTION_RULE_MAP_FILE_KEY);
    String degradePath = conf.get(DFSConfigKeys.DFS_ZONEMOVER_DEGRADE_RULE_MAP_FILE_KEY);
    String defaultPath = conf.get(DFSConfigKeys.DFS_ZONEMOVER_DEFAULT_RULE_MAP_FILE_KEY);
    load(mapPath, degradePath, defaultPath);
    LOG.info("ZoneMover migration will follow the following rule map:\n" +
        "Distribution and rule map:\n {}\nDegrade rule map:\n {}\n Upgrade rule map:\n {}\n" +
        "Default rule map:\n {}\n",
        ruleMap, degradeRuleMap, upgradeRuleMap, defaultRuleMap);
  }

  /**
   * Load rule map from the given config file
   * */
  private void load(String mapPath, String degradePath, String defaultPath) throws IOException {
    BufferedReader mapBuffer = new BufferedReader(new FileReader(mapPath));
    BufferedReader degradeBuffer = new BufferedReader(new FileReader(degradePath));
    BufferedReader defaultBuffer = new BufferedReader(new FileReader(defaultPath));
    String str;
    while ((str = mapBuffer.readLine()) != null) {
      try {
        String[] mapPair = str.trim().split("\\s+");
        ruleMap.put(ReplicationRule.parseFromString(mapPair[0]),
            ReplicationRule.parseFromString(mapPair[1]));
      } catch (IndexOutOfBoundsException e) {
        LOG.warn("Line is not good: {}", str);
      }
    }
    while ((str = degradeBuffer.readLine()) != null) {
      try {
        String[] degradePair = str.trim().split("\\s+");
        degradeRuleMap.put(ReplicationRule.parseFromString(degradePair[0]),
            ReplicationRule.parseFromString(degradePair[1]));
        upgradeRuleMap.put(ReplicationRule.parseFromString(degradePair[1]),
            ReplicationRule.parseFromString(degradePair[0]));
      } catch (IndexOutOfBoundsException e) {
        LOG.warn("Line is not good: {}", str);
      }
    }
    while ((str = defaultBuffer.readLine()) != null) {
      try {
        String[] defaultPair = str.trim().split("\\s+");
        defaultRuleMap.put(Short.valueOf(defaultPair[0]),
            ReplicationRule.parseFromString(defaultPair[1]));
      } catch (IndexOutOfBoundsException e) {
        LOG.warn("Line is not good: {}", str);
      }
    }
  }

  /**
   * Initialize the rule alias map, suppose that there are 3 digits to represent replicas on 3 DCs
   * First digit -> replicas in STT
   * Second digit -> replicas in TL
   * Third digit -> replicas in AT
   * */
  private void init() {
    ruleAlias.put(1, ReplicationRule.parseFromString(String.format("%s:1",
        MigrationDataCenters.AT)));
    ruleAlias.put(10, ReplicationRule.parseFromString(String.format("%s:1",
        MigrationDataCenters.TL)));
    ruleAlias.put(100, ReplicationRule.parseFromString(String.format("%s:1",
        MigrationDataCenters.STT)));
    ruleAlias.put(2, ReplicationRule.parseFromString(String.format("%s:2",
        MigrationDataCenters.AT)));
    ruleAlias.put(20, ReplicationRule.parseFromString(String.format("%s:2",
        MigrationDataCenters.TL)));
    ruleAlias.put(200, ReplicationRule.parseFromString(String.format("%s:2",
        MigrationDataCenters.STT)));
    ruleAlias.put(11, ReplicationRule.parseFromString(String.format("%s:1,%s:1",
        MigrationDataCenters.TL, MigrationDataCenters.AT)));
    ruleAlias.put(110, ReplicationRule.parseFromString(String.format("%s:1,%s:1",
        MigrationDataCenters.STT, MigrationDataCenters.TL)));
    ruleAlias.put(101, ReplicationRule.parseFromString(String.format("%s:1,%s:1",
        MigrationDataCenters.STT, MigrationDataCenters.AT)));
    ruleAlias.put(111, ReplicationRule.parseFromString(String.format("%s:1,%s:1,%s:1",
        MigrationDataCenters.STT, MigrationDataCenters.TL, MigrationDataCenters.AT)));
    ruleAlias.put(21, ReplicationRule.parseFromString(String.format("%s:2,%s:1",
        MigrationDataCenters.TL, MigrationDataCenters.AT)));
    ruleAlias.put(12, ReplicationRule.parseFromString(String.format("%s:1,%s:2",
        MigrationDataCenters.TL, MigrationDataCenters.AT)));
    ruleAlias.put(3, ReplicationRule.parseFromString(String.format("%s:3",
        MigrationDataCenters.AT)));
    ruleAlias.put(30, ReplicationRule.parseFromString(String.format("%s:3",
        MigrationDataCenters.TL)));
    ruleAlias.put(300, ReplicationRule.parseFromString(String.format("%s:3",
        MigrationDataCenters.STT)));
    ruleAlias.put(112, ReplicationRule.parseFromString(String.format("%s:1,%s:1,%s:2",
        MigrationDataCenters.STT, MigrationDataCenters.TL, MigrationDataCenters.AT)));
    ruleAlias.put(220, ReplicationRule.parseFromString(String.format("%s:2,%s:2",
        MigrationDataCenters.STT, MigrationDataCenters.TL)));
    ruleAlias.put(221, ReplicationRule.parseFromString(String.format("%s:2,%s:2,%s:1",
        MigrationDataCenters.STT, MigrationDataCenters.TL, MigrationDataCenters.AT)));
  }

  /**
   * If there is default rule for the specific replication in the map, return the map result
   * If not, keep replicas in AT and evenly distribute replicas in the STT&TL
   * */
  public ReplicationRule getDefaultRule(ReplicationRule distribution, short replication) {
    if (!(defaultRuleMap.get(replication) == null)) {
      return defaultRuleMap.get(replication);
    } else {
      short TLReplication = distribution.getReplica(MigrationDataCenters.TL.getName());
      short STTReplication = distribution.getReplica(MigrationDataCenters.STT.getName());
      short newTLReplication = (short) ((TLReplication + STTReplication) / 2);
      short newSTTReplication = (short) (STTReplication + TLReplication - newTLReplication);
      if (newTLReplication + newSTTReplication >= replication) {
        return ReplicationRule.parseFromString(String.format("%s:%d,%s:%d",
            MigrationDataCenters.STT, newSTTReplication,
            MigrationDataCenters.TL, newTLReplication));
      } else {
        return ReplicationRule.parseFromString(String.format("%s:%d,%s:%d,%s:%d",
            MigrationDataCenters.STT, newSTTReplication, MigrationDataCenters.TL,
            newTLReplication, MigrationDataCenters.AT,
            (replication - newSTTReplication - newTLReplication)));
      }
    }
  }

  /**
   * When replication = distribution replica,
   *         then get rule from map file
   * When replication != distribution replica
   * rep = 1 -> dis contains AT -> map of AT:1
   *         -> dis not contains AT -> map of TL:1
   * rep = 2 -> dis all in STT -> STT:2
   *         -> dis all in AT -> map of AT:2
   *         -> dis all in TL -> map of TL:2
   *         -> dis has replica in STT -> map of TL:2
   *         -> dis in TL&AT -> map of TL:1,AT:1
   * rep = 3 -> dis all in STT -> STT:3
   *         -> dis all in AT -> map of AT:3
   *         -> dis all in TL -> map of TL:3
   *         -> only have replicas in STT&TL -> map of TL:3
   *         -> has replica in 3 STT -> map of TL:1,AT:1
   *         -> more replica in AT -> map of TL:1,AT:2
   *         -> more replica in TL -> map of TL:2,AT:1
   * rep = 4 -> has more replica in AT -> map of TL:1,AT:2
   *         -> has less replica in AT -> map of TL:2,AT:1
   *         -> has no replica in AT -> map of TL:3
   * rep = 5 -> map of TL:2,AT:1
   * rep > 5 -> keep replica in AT, evenly distribute replica in STT/TL
   * */
  protected ReplicationRule getRuleFromDistribution(ReplicationRule distribution,
      short replication) {
    if (distribution.getReplica() == replication) {
      return ruleMap.get(distribution) == null ?
          getDefaultRule(distribution, replication) : ruleMap.get(distribution);
    } else {
      if (replication == 1) {
        if (distribution.getDatacenters().contains(MigrationDataCenters.AT.getName())) {
          return ruleMap.get(ruleAlias.get(1));
        } else {
          return ruleMap.get(ruleAlias.get(10));
        }
      } else if (replication == 2) {
        if (distribution.getDatacenters().size() == 1) {
          if (distribution.getDatacenters().contains(MigrationDataCenters.AT.getName())) {
            return ruleMap.get(ruleAlias.get(2));
          } else if (distribution.getDatacenters().contains(MigrationDataCenters.STT.getName())) {
            return ruleAlias.get(200);
          } else {
            return ruleMap.get(ruleAlias.get(20));
          }
        } else {
          if (distribution.getDatacenters().contains(MigrationDataCenters.STT.getName())) {
            return ruleMap.get(ruleAlias.get(20));
          } else {
            return ruleMap.get(ruleAlias.get(11));
          }
        }
      } else if (replication == 3) {
        if (distribution.getDatacenters().size() == 1) {
          if (distribution.getDatacenters().contains(MigrationDataCenters.AT.getName())) {
            return ruleMap.get(ruleAlias.get(3));
          } else if (distribution.getDatacenters().contains(MigrationDataCenters.STT.getName())) {
            return ruleAlias.get(300);
          } else {
            return ruleMap.get(ruleAlias.get(30));
          }
        } else if (distribution.getDatacenters().contains(MigrationDataCenters.STT.getName())) {
          if (distribution.getMainDataCenter().equals(MigrationDataCenters.STT.getName()) ||
              distribution.getMainDataCenter().equals(MigrationDataCenters.TL.getName())) {
            return ruleMap.get(ruleAlias.get(30));
          } else {
            return ruleMap.get(ruleAlias.get(11));
          }
        } else {
          if (distribution.getMainDataCenter().equals(MigrationDataCenters.AT.getName())) {
            return ruleMap.get(ruleAlias.get(12));
          } else {
            return ruleMap.get(ruleAlias.get(21));
          }
        }
      } else if (replication == 4) {
        if (distribution.getMainDataCenter().equals(MigrationDataCenters.AT.getName())) {
          return ruleMap.get(ruleAlias.get(12));
        } else if (distribution.getDatacenters().contains(MigrationDataCenters.AT.getName())) {
          return ruleMap.get(ruleAlias.get(21));
        } else {
          return ruleMap.get(ruleAlias.get(30));
        }
      } else if (replication == 5) {
        return ruleMap.get(ruleAlias.get(21));
      } else {
        short replica = (short)
            Math.ceil((distribution.getReplica(MigrationDataCenters.TL.getName()) +
                distribution.getReplica(MigrationDataCenters.STT.getName()))/2.0);
        if (replica * 2 >= replication) {
          return ReplicationRule.parseFromString(String.format("%s:%d,%s:%d",
              MigrationDataCenters.STT, (replication - replication/2),
              MigrationDataCenters.TL, replication/2));
        } else {
          return ReplicationRule.parseFromString(String.format("%s:%d,%s:%d,%s:%d",
              MigrationDataCenters.STT, replica, MigrationDataCenters.TL,
              replica, MigrationDataCenters.AT, (replication-replica*2)));
        }
      }
    }
  }

  public ReplicationRule getDegradeRule(String rule, short replication, ReplicationRule dis) {
    if (replication == 2) {
      return getDegradeRule(rule);
    } else {
      return getRuleFromDistribution(dis, replication);
    }
  }

  public ReplicationRule getDegradeRule(String rule) {
    return degradeRuleMap.get(ReplicationRule.parseFromString(rule));
  }

  public ReplicationRule getUpgradeRule(String rule, short replication, ReplicationRule dis) {
    if (replication == 3) {
      return getUpgradeRule(rule);
    } else {
      return getRuleFromDistribution(dis, replication);
    }
  }

  public ReplicationRule getUpgradeRule(String rule) {
    return upgradeRuleMap.get(ReplicationRule.parseFromString(rule));
  }

  public ReplicationRule generateRule(ReplicationRule rule, short replication,
      ReplicationRule dis) {
    if (rule == null) {
      return getRuleFromDistribution(dis, replication);
    }
    if (rule.getReplica() == 4 || rule.getReplica() == 5 ||
        (rule.getReplica() == 3 && rule.getDatacenters().size() < 3)) {
      if (replication == 3 && dis.getDatacenters().size() < 3) {
        return rule;
      } else {
        return getDegradeRule(rule.toString(), replication, dis);
      }
    } else if (rule.getReplica() == 3 || rule.getReplica() == 2) {
      if (replication == 3 && dis.getDatacenters().size() < 3) {
        return getUpgradeRule(rule.toString(), replication, dis);
      } else {
        return rule;
      }
    } else {
      LOG.warn("The setting rule is inconsistent! Will use distribution to generate rule!");
      return getRuleFromDistribution(dis, replication);
    }
  }

  /**
   * Check if block has replicas in the given DC
   * if there have already been some replicas then return null
   * rep = 1 -> null
   * rep = 2 -> dis not only in one DC -> map of TL:1,AT:1
   *         -> dis all in AT -> map of TL:1,AT:1
   *         -> target dc is AT -> map of TL:1,AT:1
   *         -> else -> map of TL:2
   * rep = 3 -> dis all in STT
   *            -> target dc is AT -> map of TL:2,AT:1
   *            -> target dc is TL -> map of TL:3
   *         -> dis has replica in STT -> map of TL:1,AT:1
   *         -> only have replicas in STT&TL && target dc is not AT -> map of TL:3
   *         -> target dc is AT -> map of TL:2,AT:1
   *         -> target dc is not AT -> map of TL:1,AT:2
   * rep = 4 -> target dc is AT -> map of TL:2,AT:1
   *         -> has replicas in AT -> map of TL:1,AT:2
   *         -> else -> map of TL:3
   * rep = 5 -> map of TL:2,AT:1
   * rep > 5 -> return null
   * */
  public ReplicationRule checkDistribution(ReplicationRule dis, short replication,
      MigrationDataCenters dc) {
    // Block has already had replica in the given data center then return null
    if (dis.getDatacenters().contains(dc.getName())) {
      return null;
    }
    // No move replica when replication == 1
    if (replication == 1) {
      return null;
    }

    if (replication == 2) {
      if (dis.getDatacenters().size() != 1 ||
          dis.getDatacenters().contains(MigrationDataCenters.AT.getName()) ||
          dc == MigrationDataCenters.AT) {
        return ruleMap.get(ruleAlias.get(11));
      } else {
        return ruleMap.get(ruleAlias.get(20));
      }
    }

    if (replication == 3) {
      if (dis.getDatacenters().contains(MigrationDataCenters.STT.getName())) {
        if (dis.getDatacenters().size() == 1) {
          if (dc == MigrationDataCenters.AT) {
            return ruleMap.get(ruleAlias.get(21));
          } else {
            return ruleMap.get(ruleAlias.get(30));
          }
        }
        // The distribution is one of the 3-replica rules, consider the file as 3-replica file.
        else if (degradeRuleMap.containsKey(dis)) {
          if (Objects.equals(dis.getMainDataCenter(), MigrationDataCenters.AT.getName())) {
            return ruleMap.get(ruleAlias.get(12));
          } else {
            return ruleMap.get(ruleAlias.get(21));
          }
        }
        return ruleMap.get(ruleAlias.get(11));
      } else if (!(dis.getDatacenters().contains(MigrationDataCenters.AT.getName())
          || dc == MigrationDataCenters.AT)) {
        return ruleMap.get(ruleAlias.get(30));
      } else if (dc == MigrationDataCenters.AT ||
          !Objects.equals(dis.getMainDataCenter(), MigrationDataCenters.AT.getName())) {
        return ruleMap.get(ruleAlias.get(21));
      } else {
        return ruleMap.get(ruleAlias.get(12));
      }
    }

    if (replication == 4) {
      if (dis.getDatacenters().contains(MigrationDataCenters.AT.getName())) {
        return ruleMap.get(ruleAlias.get(12));
      } else if (dc == MigrationDataCenters.AT) {
        return ruleMap.get(ruleAlias.get(21));
      } else {
        return ruleMap.get(ruleAlias.get(30));
      }
    }

    if (replication == 5) {
      return ruleMap.get(ruleAlias.get(21));
    }

    return null;
  }
}