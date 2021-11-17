package org.apache.hadoop.hdfs.server.zoneservice;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestReplicationRuleSection {
  
  @Test
  public void testParseFromString() {
    String stringSection = "/sg_dc:3";
    ReplicationRuleSection section = 
        new ReplicationRuleSection("/sg_dc", (short)3);
    assertEquals(section,
        ReplicationRuleSection.parseFromString(stringSection));

    // must have 2 fields
    stringSection = "/sg_dc";
    try {
      ReplicationRuleSection.parseFromString(stringSection);
      fail("IllegalArgumentException expected!");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("must have 2 fields"));
    }

    // datacenter must start with '/'
    // must have 2 fields
    stringSection = "sg_dc:3";
    try {
      ReplicationRuleSection.parseFromString(stringSection);
      fail("IllegalArgumentException expected!");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("must starts with"));
    }
  }
}
