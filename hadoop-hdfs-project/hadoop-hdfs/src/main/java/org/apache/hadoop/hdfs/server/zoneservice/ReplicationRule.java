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

import com.google.common.base.Joiner;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** ReplicationRule is a class that defines the distribution of replicas
 * in a multiple datacenter environment. A ReplicationRule is composed of one 
 * or multiple {@link ReplicationRuleSection}.
 */
public class ReplicationRule {
  private final Set<ReplicationRuleSection> sections;
  private final Set<String> datacenters;
  // Characters to strip
  private final static String[] STRIP_CHARACTERS = {" "};
  // "," is the separator of pattern "/dc1:replica1,/dc2:replica2"
  public final static String SECTION_SEPARATOR = ",";

  ReplicationRule() {
    this.sections = new HashSet<>();
    this.datacenters = new HashSet<>();
  }

  /**
   * Construct the ReplicationRule instance from a string format rule.
   * @param rule a string follows the pattern
   *             "/dc1:replica1,/dc2:replica2"
   * @return the constructed ReplicationRule
   */
  public static ReplicationRule parseFromString(final String rule)
      throws IllegalArgumentException{
    if (rule == null) {
      throw new IllegalArgumentException(
          "ReplicationRule string cannot be null");
    }
    ReplicationRule replicationRule = new ReplicationRule();

    // Strip
    String strip = rule;
    for (String s: STRIP_CHARACTERS) {
      strip = strip.replace(s, "");
    }

    // Split
    String[] sections = strip.split(SECTION_SEPARATOR);
    for (String section: sections) {
      replicationRule.addSection(
          ReplicationRuleSection.parseFromString(section));
    }
    return replicationRule;
  }

  /**
   * Construct a ReplicationRule instance from a map.
   * @param map key represents datacenter, value represents replica factor
   * @return the constructed ReplicationRule
   */
  public static ReplicationRule parseFromMap(
      @Nonnull final Map<String, Short> map) {
    ReplicationRule rule = new ReplicationRule();
    for (Map.Entry<String, Short> entry: map.entrySet()) {
      rule.addSection(
          new ReplicationRuleSection(entry.getKey(), entry.getValue()));
    }
    return rule;
  }

  /**
   * Convert the rule to a map.
   */
  public Map<String, Short> toMap() {
    Map<String, Short> map = new HashMap<>();
    for (ReplicationRuleSection section: sections) {
      map.put(section.getDataCenter(), section.getReplica());
    }
    return map;
  }

  /**
   * Add a section
   * @param section the section to add
   */
  private void addSection(final ReplicationRuleSection section) {
    this.sections.add(section);
    this.datacenters.add(section.getDataCenter());
  }

  public Set<ReplicationRuleSection> getSections() {
    return sections;
  }

  public Set<String> getDatacenters() {
    return datacenters;
  }

  /**
   * Get the sum replica of all sections.
   * @return replica num
   */
  public short getReplica() {
    short replica = (short) 0;
    for (ReplicationRuleSection s: sections) {
      replica += s.getReplica();
    }
    return replica;
  }

  @Override
  public String toString() {
    return Joiner.on(",").join(sections);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ReplicationRule that = (ReplicationRule) o;
    return Objects.equals(sections, that.sections);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sections);
  }
}