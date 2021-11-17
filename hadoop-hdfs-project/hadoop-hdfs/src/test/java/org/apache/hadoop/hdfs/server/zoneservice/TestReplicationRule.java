package org.apache.hadoop.hdfs.server.zoneservice;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestReplicationRule {
  
  @Test
  public void testParseFromString() {
    String stringRule = "/dc1:1,/dc2:2";
    ReplicationRule rule = ReplicationRule.parseFromString(stringRule);
    Set<ReplicationRuleSection> sections = rule.getSections();
    assertEquals(2, sections.size());
    assertTrue(sections.contains(
        new ReplicationRuleSection("/dc1", (short)1)));
    assertTrue(sections.contains(
        new ReplicationRuleSection("/dc2", (short)2)));
    assertEquals(3, rule.getReplica());
  }

  @Test
  public void testParseFromMap() {
    Map<String, Short> map = new HashMap<>();
    map.put("/dc1", (short) 1);
    map.put("/dc2", (short) 2);
    ReplicationRule rule = ReplicationRule.parseFromMap(map);
    Set<ReplicationRuleSection> sections = rule.getSections();
    assertEquals(2, sections.size());
    assertTrue(sections.contains(
        new ReplicationRuleSection("/dc1", (short)1)));
    assertTrue(sections.contains(
        new ReplicationRuleSection("/dc2", (short)2)));
    assertEquals(3, rule.getReplica());
  }

  @Test
  public void testToMap() {
    Map<String, Short> map = new HashMap<>();
    map.put("/dc1", (short) 1);
    map.put("/dc2", (short) 2);
    ReplicationRule rule = ReplicationRule.parseFromMap(map);
    assertEquals(map, rule.toMap());
  }
}
