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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestReplicationRuleSection {

  @Test
  public void testParseFromString() {
    String stringSection = "(/sg_dc, 3)";
    ReplicationRuleSection section =
        new ReplicationRuleSection("/sg_dc", (short)3);
    assertEquals(section,
        ReplicationRuleSection.parseFromString(stringSection));

    // must have 2 fields
    stringSection = "(/sg_dc)";
    try {
      ReplicationRuleSection.parseFromString(stringSection);
      fail("IllegalArgumentException expected!");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("must have 2 fields"));
    }

    // datacenter must start with '/'
    // must have 2 fields
    stringSection = "(sg_dc, 3)";
    try {
      ReplicationRuleSection.parseFromString(stringSection);
      fail("IllegalArgumentException expected!");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("must starts with"));
    }
  }
}