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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttrCodec;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** ReplicationRuleUtil is an Util class for ReplicationRule.
 */
public class ReplicationRuleUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationRuleUtil.class);
  private final FileSystem fs;
  private final List<String> dcPool;
  private static final String ATTR_KEY =
      StringUtils.toLowerCase(DFSConfigKeys.DFS_ZONE_REPLICATION_RULE_XATTR_NAMESPACE.name()) +
          "." + DFSConfigKeys.DFS_ZONE_REPLICATION_RULE_XATTR_NAME;
  private static final ConcurrentHashMap<String, ReplicationRule> rules =
      new ConcurrentHashMap<>();

  public ReplicationRuleUtil(FileSystem fs) {
    Preconditions.checkNotNull(fs);
    this.fs = fs;
    String dc = fs.getConf().get(DFSConfigKeys.DFS_ZONESERVICE_AUTO_BALANCER_DATACENTERS_KEY);
    dcPool = Arrays.asList(dc.split(","));
  }

  /**
   * Get the replicationRule of the corresponding path.
   * @param src file or directory path
   * @return the ReplicationRule of the file/directory
   * @throws IOException if the file/directory does not have ReplicationRule or encounters
   *                     other exceptions
   */
  public ReplicationRule getRuleFromXAttr(String src) throws IOException {
    return this.getRuleFromXAttr(new Path(src));
  }

  /**
   * Get the replicationRule of the corresponding path.
   * @param path file or directory
   * @return the ReplicationRule of the file/directory
   * @throws IOException if the file/directory does not have ReplicationRule or encounters
   *                     other exceptions
   */
  public ReplicationRule getRuleFromXAttr(Path path) throws IOException {
    byte[] bs = fs.getXAttr(path, ATTR_KEY);
    String value = XAttrCodec.encodeValue(bs, XAttrCodec.TEXT);
    if (!rules.containsKey(value)) {
      rules.put(value, ReplicationRule.parseFromString(value.replace("\"", "")));
    }
    return rules.get(value);
  }

  /**
   * Get the content of the rule key without parsing it.
   * @param src file or directory path
   * @return the content of the key
   * @throws IOException if the file/directory does not have content of the key or
   *                     encounters other exceptions
   */
  public String getStringFromRuleKey(String src) throws IOException {
    return this.getStringFromRuleKey(new Path(src));
  }

  /**
   * Get the content of the rule key without parsing it.
   * @param path file or directory path
   * @return the content of the key
   * @throws IOException if the file/directory does not have content of the key or
   *                     encounters other exceptions
   */
  public String getStringFromRuleKey(Path path) throws IOException {
    byte[] bs = fs.getXAttr(path, ATTR_KEY);
    return XAttrCodec.encodeValue(bs, XAttrCodec.TEXT);
  }

  /**
   * Get the content of the rule key by file id without parsing it.
   * @param src file or directory path, just used for getting ns
   * @param fileId get xattr of the file with this file id
   * @return the content of the key
   * @throws IOException if the file/directory does not have content of the key or
   *                     encounters other exceptions
   */
  public String getStringFromRuleKey(String src, long fileId) throws IOException {
    return this.getStringFromRuleKey(new Path(src), fileId);
  }

  /**
   * Get the content of the rule key by file id without parsing it.
   * @param path file or directory path
   * @return the content of the key
   * @throws IOException if the file/directory does not have content of the key or
   *                     encounters other exceptions
   */
  public String getStringFromRuleKey(Path path, long fileId) throws IOException {
    byte[] bs = fs.getXAttr(path, fileId, ATTR_KEY);
    return XAttrCodec.encodeValue(bs, XAttrCodec.TEXT);
  }

  /**
   * Check if the file/directory has ReplicationRule in its XAttr.
   * @param src file or directory path
   * @return yes or no
   */
  public boolean hasRuleInXAttr(String src) throws IOException {
    return this.hasRuleInXAttr(new Path(src));
  }

  /**
   * Check if the file/directory has ReplicationRule in its XAttr.
   * @param path file or directory
   * @return yes or no
   */
  public boolean hasRuleInXAttr(Path path) throws IOException {
    return fs.listXAttrs(path).contains(ATTR_KEY);
  }

  /**
   * Set the replicationRule to the file/directory's XAttr.
   * @param src file or directory path
   * @param rule replicationRule instance
   */
  public void setRuleToXAttr(String src, ReplicationRule rule) throws IOException {
    setRuleToXAttr(new Path(src), rule);
  }

  /**
   * Set the replicationRule to the file/directory's XAttr.
   * @param path file or directory
   * @param rule replicationRule instance
   */
  public void setRuleToXAttr(Path path, ReplicationRule rule) throws IOException {
    fs.setXAttr(path, ATTR_KEY, XAttrCodec.decodeValue(rule.toString()));
  }

  /**
   * Set the replicationRule to the file/directory's XAttr by file ID.
   * @param pathName file or directory path, just used for getting ns
   * @param rule replicationRule instance
   * @param fileId set xattr on the file with this file id
   */
  public void setRuleToXAttr(String pathName, ReplicationRule rule, long fileId) throws IOException {
    fs.setXAttr(new Path(pathName), ATTR_KEY, XAttrCodec.decodeValue(rule.toString()), fileId);
  }

  /**
   * Auto generate replica rule to make replicas of a file saved in two DCs
   * then set rule into file Xattr
   * The file will be recognized by file id not path name
   */
  public void autoSetReplicaRule(String pathName, long fileId) throws IOException {
    BlockLocation[] blockLocations = fs.getFileBlockLocationsByFileId(new Path(pathName), fileId);

    if (blockLocations.length == 0) {
      throw new IOException("Get block location failed for " + pathName + " (" + fileId + ")");
    }
    int replicaNum = fs.listStatusById(new Path(pathName), fileId)[0].getReplication();
    ReplicationRule rule = generateRule(blockLocations, replicaNum);
    LOG.info("Set rule {} on path {} with id {}", rule, pathName, fileId);
    setRuleToXAttr(pathName, rule, fileId);
  }

  /**
   * Get main data center of the file
   */
  private String[] getMainDC(BlockLocation[] blockLocations)
      throws IOException {
    Map<String, String> mainDCPair = new HashMap<>();
    String tmpMainDC = "";
    String tmpNonMainDC = "";
    for (BlockLocation blockLocation: blockLocations) {
      String[] locs = blockLocation.getTopologyPaths();
      Map<String, Integer> dcReplica = new HashMap<>();
      int mainReplicaNum = 0;
      tmpMainDC = "";
      tmpNonMainDC = "";
      // Count replica's data center
      for (String loc: locs) {
        String dc = "/" + loc.split("/")[1];
        if (dcReplica.containsKey(dc)) {
          dcReplica.put(dc, dcReplica.get(dc) + 1);
        } else {
          dcReplica.put(dc, 1);
        }
      }

      for (String dc: dcReplica.keySet()) {
        int curReplicaNum = dcReplica.get(dc);
        if (curReplicaNum > mainReplicaNum) {
          tmpMainDC = dc;
          mainReplicaNum = curReplicaNum;
        } else {
          tmpNonMainDC = dc;
        }
      }

      // When one of the main DC appears for twice, then will return this main DC
      // Meanwhile tool will try to return a non-empty non-main DC instead of ""
      if (mainDCPair.keySet().contains(tmpMainDC)) {
        // Will ignore the scenario that replicas for one file placed in 3 different DCs
        // (two different non-main DC correspond to the same main DC)
        return new String[]{tmpMainDC,
            (tmpNonMainDC.isEmpty() ? mainDCPair.get(tmpMainDC) : tmpNonMainDC)};
      } else {
        mainDCPair.put(tmpMainDC, tmpNonMainDC);
      }
    }
    // No main DC appears for twice.
    return new String[]{tmpMainDC, tmpNonMainDC};
  }

  public ReplicationRule generateRule(BlockLocation[] blockLocations, int replica)
      throws IOException {
    StringBuilder ruleString = new StringBuilder();
    int replicaNum = replica <= 0 ? blockLocations[0].getHosts().length : replica;
    // Replication statics
    String[] dcList = getMainDC(blockLocations);
    String mainDC = dcList[0];
    String nonMainDC = dcList[1];

    if (nonMainDC.isEmpty()) {
      try {
        nonMainDC = dcPool.get(0).equals(mainDC) ? dcPool.get(1) : dcPool.get(0);
      } catch (IndexOutOfBoundsException e) {
        LOG.error("No enough dc can be chosen to set rule for files! Please check {}!",
            DFSConfigKeys.DFS_ZONESERVICE_AUTO_BALANCER_DATACENTERS_KEY);
      }
    }

    if (replicaNum > 3) {
      ruleString.append(nonMainDC)
          .append(ReplicationRuleSection.FIELD_SEPARATOR)
          .append(2)
          .append(ReplicationRule.SECTION_SEPARATOR)
          .append(mainDC)
          .append(ReplicationRuleSection.FIELD_SEPARATOR)
          .append(replicaNum - 2);
    } else if (replicaNum > 1) {
      ruleString.append(nonMainDC)
          .append(ReplicationRuleSection.FIELD_SEPARATOR)
          .append(1)
          .append(ReplicationRule.SECTION_SEPARATOR)
          .append(mainDC)
          .append(ReplicationRuleSection.FIELD_SEPARATOR)
          .append(replicaNum - 1);
    } else {
      ruleString.append(mainDC)
          .append(ReplicationRuleSection.FIELD_SEPARATOR)
          .append(1);
    }

    return ReplicationRule.parseFromString(ruleString.toString());
  }
}
