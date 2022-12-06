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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttrCodec;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/** ReplicationRuleUtil is an Util class for ReplicationRule.
 */
public class ReplicationRuleUtil {
  private final FileSystem fs;
  private static final String ATTR_KEY =
      StringUtils.toLowerCase(DFSConfigKeys.DFS_ZONE_REPLICATION_RULE_XATTR_NAMESPACE.name()) +
          "." + DFSConfigKeys.DFS_ZONE_REPLICATION_RULE_XATTR_NAME;
  private static final ConcurrentHashMap<String, ReplicationRule> rules =
      new ConcurrentHashMap<>();

  public ReplicationRuleUtil(FileSystem fs) {
    Preconditions.checkNotNull(fs);
    this.fs = fs;
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
}
