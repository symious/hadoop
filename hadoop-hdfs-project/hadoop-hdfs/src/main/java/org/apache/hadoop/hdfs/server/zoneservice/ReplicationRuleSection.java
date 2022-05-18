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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/** ReplicationRuleSection is a class that defines the replica
 *  in a specific data center.
 */
public class ReplicationRuleSection {
  // Replica is data center specific
  private final String dataCenter;
  // Replica value in a specific data center
  private final short replica;
  // Characters to strip
  private final static String[] STRIP_CHARACTERS = {" "};
  private final static String FIELD_SEPARATOR = ":";
  private static final String ROOT = "/";
  private final static Logger LOG =
      LoggerFactory.getLogger(ReplicationRuleSection.class);

  ReplicationRuleSection(final String dataCenter, final short replica) {
    this.dataCenter = dataCenter;
    this.replica = replica;
  }

  /**
   * Construct the ReplicationRuleSection instance from a string format rule.
   * @param rule a string follows the pattern "dc:replica".
   *             For example, "/sg_dc:3".
   * @return the constructed ReplicationRuleSection
   */
  public static ReplicationRuleSection parseFromString(final String rule)
      throws IllegalArgumentException {
    if (rule == null) {
      throw new IllegalArgumentException(
          "ReplicationRuleSection string cannot be null");
    }

    LOG.info("Trying to parse ReplicationRuleSection from {" + rule + "} ...");
    // Strip
    String strip = rule;
    for (String s: STRIP_CHARACTERS) {
      strip = strip.replace(s, "");
    }

    // Split and check
    String[] fields = strip.split(FIELD_SEPARATOR);
    if (fields.length != 2) {
      throw new IllegalArgumentException(
          "ReplicationRuleSection must have 2 fields!");
    }

    if (!fields[0].startsWith(ROOT)) {
      throw new IllegalArgumentException(
          "dataCenter in ReplicationRuleSection must starts with '" + ROOT + "'");
    }

    return new ReplicationRuleSection(
        fields[0], Short.parseShort(fields[1]));
  }

  public String getDataCenter() {
    return dataCenter;
  }

  public short getReplica() {
    return replica;
  }

  @Override
  public String toString() {
    return dataCenter + ":" + replica;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ReplicationRuleSection that = (ReplicationRuleSection) o;
    return replica == that.replica
        && dataCenter.equals(that.dataCenter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dataCenter, replica);
  }
}