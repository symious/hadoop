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

package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrCodec;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.zoneservice.ReplicationRule;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/** FSDirXAttrReplicationRuleOp is used to check ReplicationRule stored in XAttr.
 */
public class FSDirXAttrReplicationRuleOp {
  private static final XAttr.NameSpace NAMESPACE =
      DFSConfigKeys.DFS_ZONE_REPLICATION_RULE_XATTR_NAMESPACE;
  private static final String NAME =
      DFSConfigKeys.DFS_ZONE_REPLICATION_RULE_XATTR_NAME;
  private static final String ATTR_KEY =
      StringUtils.toLowerCase(NAMESPACE.name()) + "." + NAME;
  private static final ConcurrentHashMap<String, ReplicationRule> rules =
      new ConcurrentHashMap<>();
  private static final Logger LOG =
      LoggerFactory.getLogger(FSDirXAttrReplicationRuleOp.class);

  /**
   * Get the replicationRule of the inode.
   *
   * @param fsd FSDirectory to lock
   * @param iNodeFile iNodeFile to get
   * @return ReplicationRule or null
   */
  public static ReplicationRule getRuleFromInodeFile(
      FSDirectory fsd, INodeFile iNodeFile) {
    List<XAttr> xAttrs;
    fsd.readLock();
    try {
      xAttrs = XAttrStorage.readINodeXAttrs(iNodeFile);
    } finally {
      fsd.readUnlock();
    }
    return getRuleFromXAttrs(xAttrs);
  }

  /**
   * Get the replicationRule from the list of XAttr.
   * @param xAttrs the list of XAttr
   * @return ReplicationRule or null if (no rule exist)/(encounters exception)
   */
  public static ReplicationRule getRuleFromXAttrs(List<XAttr> xAttrs) {
    if (xAttrs == null) {
      // Reasons to return "null" here:
      // 1. If we throw IOException here, then NameNode prefers to call
      //   "hasRuleInXAttr" first and calls "getRuleFromXAttr" second.
      //   This needs to call "readINodeXAttrs" two times.
      // 2. We need to keep compatible with JDK 1.7, so do not use
      //   java.util.Optional (since 1.8) here.
      return null;
    }

    for (XAttr xAttr: xAttrs) {
      if (xAttr.getNameSpace().equals(NAMESPACE) && xAttr.getName().equals(NAME)) {
        try {
          String value = XAttrCodec.encodeValue(
              xAttr.getValue(), XAttrCodec.TEXT);
          if (!rules.containsKey(value)) {
            try {
              rules.put(value, ReplicationRule.parseFromString(
                  value.replace("\"", "")));
            } catch (IllegalArgumentException e) {
              LOG.warn(String.format("Illegal ReplicationRule: %s",
                  value.replace("\"", "")));
              return null;
            }
          }
          return rules.get(value);
        } catch (IOException e) {
          LOG.warn(String.format("Failed to encode name=%s", ATTR_KEY));
          return null;
        }
      }
    }

    return null;
  }

  /**
   * Check if the inode has ReplicationRule in its XAttr.
   *
   * @param fsd FSDirectory to lock
   * @param iNodeFile iNodeFile to check
   * @return yes or no
   */
  public static boolean hasRuleInXAttr(FSDirectory fsd, INodeFile iNodeFile) {
    List<XAttr> xAttrs;
    fsd.readLock();
    try {
      xAttrs = XAttrStorage.readINodeXAttrs(iNodeFile);
    } finally {
      fsd.readUnlock();
    }

    if (xAttrs == null) {
      return false;
    }

    for (XAttr xAttr: xAttrs) {
      if (xAttr.getNameSpace().equals(NAMESPACE) && xAttr.getName().equals(NAME)) {
        return true;
      }
    }
    return false;
  }
}