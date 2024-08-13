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
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ZONEMOVER_DISTRIBUTION_RULE_MAP_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ZONE_SUPPORT_MIGRATE_REPLICA_RULES_KEY;
import static org.junit.Assert.assertNull;

public class TestMigrationRuleMap {

  private final static String TEST_RULE_MAP_FILE1 = "testRuleMapFile1";
  private final static String TEST_RULE_MAP_FILE2 = "testRuleMapFile2";
  private final static String TEST_RULE_MAP_FILE3 = "testRuleMapFile3";
  private final static String TEST_DECREASE_RULE_MAP_FILE = "testDecreaseRuleMapFile";

  @Test
  public void testParseFromRuleMap() throws IOException {
    Configuration conf  = new Configuration();

    // Test increase replication.
    // Test from /AirTrunk to /YTL.
    String sourceDC = "/AirTrunk";
    String targetDC = "/YTL";
    boolean isDecrease = false;
    short replication = 1;
    String ruleMapFile = Objects.requireNonNull(TestMigrationRuleMap.class.getClassLoader()
        .getResource(TEST_RULE_MAP_FILE1)).getPath();
    conf.set(DFS_ZONEMOVER_DISTRIBUTION_RULE_MAP_FILE_KEY, ruleMapFile);
    MigrationRuleMap migrationRuleMap = new MigrationRuleMap(conf);
    ReplicationRule distribution = ReplicationRule.parseFromString("/AirTrunk:1");
    ReplicationRule expectDistribution = migrationRuleMap.getRuleFromDistribution(
        distribution, replication, sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/YTL:1"), expectDistribution);

    replication = 2;
    distribution = ReplicationRule.parseFromString("/AirTrunk:1,/STT:1");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(
        distribution, replication, sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/AirTrunk:1,/STT:1,/YTL:1"),
        expectDistribution);

    distribution = ReplicationRule.parseFromString("/AirTrunk:2");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(
        distribution, replication, sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/AirTrunk:1,/YTL:1"),
        expectDistribution);

    replication = 3;
    distribution = ReplicationRule.parseFromString("/AirTrunk:3");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(distribution, replication,
        sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/AirTrunk:1,/YTL:2"), expectDistribution);

    distribution = ReplicationRule.parseFromString("/AirTrunk:2,/STT:1");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(distribution, replication,
        sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/AirTrunk:1,/STT:1,/YTL:2"),
        expectDistribution);

    distribution = ReplicationRule.parseFromString("/AirTrunk:1,/STT:2");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(distribution, replication,
        sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/AirTrunk:1,/STT:2,/YTL:1"),
        expectDistribution);

    replication = 4;
    distribution = ReplicationRule.parseFromString("/AirTrunk:1,/STT:3");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(distribution, replication,
        sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/AirTrunk:1,/YTL:1,/STT:2"),
        expectDistribution);

    distribution = ReplicationRule.parseFromString("/AirTrunk:3,/STT:1");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(distribution, replication,
        sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/AirTrunk:1,/STT:1,/YTL:2"),
        expectDistribution);

    replication = 5;
    distribution = ReplicationRule.parseFromString("/AirTrunk:5");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(distribution, replication,
        sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/AirTrunk:2,/YTL:3"), expectDistribution);

    // Test from /STT to /AirTrunk.
    sourceDC = "/STT";
    targetDC = "/AirTrunk";
    replication = 1;
    ruleMapFile = Objects.requireNonNull(TestMigrationRuleMap.class.getClassLoader()
        .getResource(TEST_RULE_MAP_FILE2)).getPath();
    conf.set(DFS_ZONEMOVER_DISTRIBUTION_RULE_MAP_FILE_KEY, ruleMapFile);
    migrationRuleMap = new MigrationRuleMap(conf);
    distribution = ReplicationRule.parseFromString("/STT:1");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(
        distribution, replication, sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/AirTrunk:1"), expectDistribution);

    replication = 2;
    distribution = ReplicationRule.parseFromString("/STT:2");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(
        distribution, replication, sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/AirTrunk:1,/STT:1"),
        expectDistribution);

    replication = 3;
    distribution = ReplicationRule.parseFromString("/STT:3");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(
        distribution, replication, sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/AirTrunk:2,/STT:1"),
        expectDistribution);

    distribution = ReplicationRule.parseFromString("/STT:2,/AirTrunk:1");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(distribution, replication,
        sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/AirTrunk:2,/STT:1"), expectDistribution);

    distribution = ReplicationRule.parseFromString("/STT:1,/YTL:2");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(distribution, replication,
        sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/AirTrunk:1,/STT:1,/YTL:2"),
        expectDistribution);

    distribution = ReplicationRule.parseFromString("/STT:2,/YTL:1");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(distribution, replication,
        sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/AirTrunk:2,/STT:1,/YTL:1"),
        expectDistribution);

    replication = 5;
    distribution = ReplicationRule.parseFromString("/STT:5");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(distribution, replication,
        sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/AirTrunk:3,/STT:2"), expectDistribution);

    replication = 6;
    distribution = ReplicationRule.parseFromString("/STT:6");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(distribution, replication,
        sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/AirTrunk:3,/STT:3"), expectDistribution);

    // Test from /STT to /YTL.
    sourceDC = "/STT";
    targetDC = "/YTL";
    replication = 1;
    ruleMapFile = Objects.requireNonNull(TestMigrationRuleMap.class.getClassLoader()
        .getResource(TEST_RULE_MAP_FILE3)).getPath();
    conf.set(DFS_ZONEMOVER_DISTRIBUTION_RULE_MAP_FILE_KEY, ruleMapFile);
    migrationRuleMap = new MigrationRuleMap(conf);
    distribution = ReplicationRule.parseFromString("/STT:1");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(
        distribution, replication, sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/YTL:1"), expectDistribution);

    replication = 2;
    distribution = ReplicationRule.parseFromString("/STT:1,/AirTrunk:1");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(
        distribution, replication, sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/AirTrunk:1,/STT:1,/YTL:1"),
        expectDistribution);

    distribution = ReplicationRule.parseFromString("/STT:2");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(
        distribution, replication, sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/STT:1,/YTL:1"),
        expectDistribution);

    replication = 3;
    distribution = ReplicationRule.parseFromString("/STT:3");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(
        distribution, replication, sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/STT:1,/YTL:2"),
        expectDistribution);

    distribution = ReplicationRule.parseFromString("/STT:2,/AirTrunk:1");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(distribution, replication,
        sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/STT:1,/YTL:2,/AirTrunk:1"),
        expectDistribution);

    distribution = ReplicationRule.parseFromString("/STT:1,/AirTrunk:2");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(distribution, replication,
        sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/AirTrunk:2,/STT:1,/YTL:1"),
        expectDistribution);

    distribution = ReplicationRule.parseFromString("/AirTrunk:1,/STT:1,/YTL:1");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(distribution, replication,
        sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/AirTrunk:1,/STT:1,/YTL:1"),
        expectDistribution);

    replication = 4;
    distribution = ReplicationRule.parseFromString("/AirTrunk:1,/YTL:2,/STT:1");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(distribution, replication,
        sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/AirTrunk:1,/YTL:2,/STT:1"),
        expectDistribution);

    distribution = ReplicationRule.parseFromString("/AirTrunk:3,/STT:1");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(distribution, replication,
        sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/AirTrunk:2,/YTL:1,/STT:1"),
        expectDistribution);

    replication = 5;
    distribution = ReplicationRule.parseFromString("/STT:5");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(distribution, replication,
        sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/YTL:3,/STT:2"), expectDistribution);

    distribution = ReplicationRule.parseFromString("/STT:1,/YTL:1,/AirTrunk:3");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(distribution, replication,
        sourceDC, targetDC, isDecrease);
    assertNull("Expected distribution should be null", expectDistribution);

    replication = 6;
    distribution = ReplicationRule.parseFromString("/STT:6");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(distribution, replication,
        sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/YTL:3,/STT:3"), expectDistribution);

    distribution = ReplicationRule.parseFromString("/AirTrunk:3,/STT:3");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(distribution, replication,
        sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/AirTrunk:3,/YTL:2,/STT:1"),
        expectDistribution);

    distribution = ReplicationRule.parseFromString("/AirTrunk:2,/YTL:2,/STT:2");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(distribution, replication,
        sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/AirTrunk:2,/YTL:3,/STT:1"),
        expectDistribution);

    // Test decrease replication.
    // Test from /STT to /AirTrunk.
    sourceDC = "/STT";
    targetDC = "/AirTrunk";
    replication = 2;
    isDecrease = true;
    ruleMapFile = Objects.requireNonNull(TestMigrationRuleMap.class.getClassLoader()
        .getResource(TEST_DECREASE_RULE_MAP_FILE)).getPath();
    conf.set(DFS_ZONEMOVER_DISTRIBUTION_RULE_MAP_FILE_KEY, ruleMapFile);
    migrationRuleMap = new MigrationRuleMap(conf);
    distribution = ReplicationRule.parseFromString("/AirTrunk:1,/STT:1");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(
        distribution, replication, sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/AirTrunk:2"), expectDistribution);

    replication = 3;
    distribution = ReplicationRule.parseFromString("/AirTrunk:2,/STT:1");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(
        distribution, replication, sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/AirTrunk:3"), expectDistribution);

    replication = 4;
    distribution = ReplicationRule.parseFromString("/AirTrunk:2,/STT:2");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(
        distribution, replication, sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/AirTrunk:4"), expectDistribution);

    replication = 5;
    distribution = ReplicationRule.parseFromString("/AirTrunk:3,/STT:2");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(
        distribution, replication, sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/AirTrunk:5"), expectDistribution);

    // Test from /STT to /YTL.
    sourceDC = "/STT";
    targetDC = "/YTL";
    replication = 1;
    distribution = ReplicationRule.parseFromString("/YTL:1");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(
        distribution, replication, sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/YTL:1"), expectDistribution);

    replication = 2;
    distribution = ReplicationRule.parseFromString("/STT:1,/YTL:1");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(
        distribution, replication, sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/YTL:2"), expectDistribution);

    replication = 3;
    distribution = ReplicationRule.parseFromString("/STT:1,/YTL:1,/AirTrunk:1");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(
        distribution, replication, sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/AirTrunk:1,/YTL:1"), expectDistribution);

    distribution = ReplicationRule.parseFromString("/STT:1,/YTL:2");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(
        distribution, replication, sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/YTL:3"), expectDistribution);

    replication = 4;
    distribution = ReplicationRule.parseFromString("/STT:1,/YTL:2,/AirTrunk:1");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(
        distribution, replication, sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/AirTrunk:1,/YTL:2"), expectDistribution);

    distribution = ReplicationRule.parseFromString("/STT:1,/YTL:1,/AirTrunk:2");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(
        distribution, replication, sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/YTL:1,/AirTrunk:2"), expectDistribution);

    replication = 6;
    distribution = ReplicationRule.parseFromString("/STT:2,/YTL:2,/AirTrunk:2");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(
        distribution, replication, sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/YTL:4,/AirTrunk:2"), expectDistribution);

    distribution = ReplicationRule.parseFromString("/STT:3,/YTL:3");
    expectDistribution = migrationRuleMap.getRuleFromDistribution(
        distribution, replication, sourceDC, targetDC, isDecrease);
    Assert.assertEquals(ReplicationRule.parseFromString("/YTL:6"), expectDistribution);


    // Test getRuleFromDistributionWithZS then the replicas are distributed among three IDC.
    conf.set(DFS_ZONE_SUPPORT_MIGRATE_REPLICA_RULES_KEY,
        "2=/AirTrunk:1,/YTL:1,/STT:1;3=/AirTrunk:1,/YTL:2,/STT:1");
    Set<String> validDataCenters = new HashSet<>(Arrays.asList("/AirTrunk", "/YTL", "/STT"));
    migrationRuleMap = new MigrationRuleMap(conf);
    replication = 2;
    distribution = ReplicationRule.parseFromString("/STT:2");
    expectDistribution = migrationRuleMap.getRuleFromDistributionWithZS(distribution, replication,
        validDataCenters);
    Assert.assertEquals(ReplicationRule.parseFromString("/AirTrunk:1,/YTL:1,/STT:1"),
        expectDistribution);

    distribution = ReplicationRule.parseFromString("/STT:1,/AirTrunk:1");
    expectDistribution = migrationRuleMap.getRuleFromDistributionWithZS(distribution, replication,
        validDataCenters);
    Assert.assertEquals(ReplicationRule.parseFromString("/AirTrunk:1,/YTL:1,/STT:1"),
        expectDistribution);

    replication = 3;
    distribution = ReplicationRule.parseFromString("/STT:3");
    expectDistribution = migrationRuleMap.getRuleFromDistributionWithZS(distribution, replication,
        validDataCenters);
    Assert.assertEquals(ReplicationRule.parseFromString("/AirTrunk:1,/YTL:2,/STT:1"),
        expectDistribution);

    distribution = ReplicationRule.parseFromString("/STT:2,/AirTrunk:1");
    expectDistribution = migrationRuleMap.getRuleFromDistributionWithZS(distribution, replication,
        validDataCenters);
    Assert.assertEquals(ReplicationRule.parseFromString("/AirTrunk:1,/YTL:2,/STT:1"),
        expectDistribution);

    replication = 5;
    distribution = ReplicationRule.parseFromString("/STT:5");
    expectDistribution = migrationRuleMap.getRuleFromDistributionWithZS(distribution, replication,
        validDataCenters);
    Assert.assertEquals(ReplicationRule.parseFromString("/AirTrunk:1,/YTL:1,/STT:3"),
        expectDistribution);

    distribution = ReplicationRule.parseFromString("/STT:3,/AirTrunk:2");
    expectDistribution = migrationRuleMap.getRuleFromDistributionWithZS(distribution, replication,
        validDataCenters);
    Assert.assertEquals(ReplicationRule.parseFromString("/AirTrunk:2,/YTL:1,/STT:2"),
        expectDistribution);

    replication = 6;
    distribution = ReplicationRule.parseFromString("/STT:6");
    expectDistribution = migrationRuleMap.getRuleFromDistributionWithZS(distribution, replication,
        validDataCenters);
    Assert.assertEquals(ReplicationRule.parseFromString("/AirTrunk:1,/YTL:1,/STT:4"),
        expectDistribution);

    distribution = ReplicationRule.parseFromString("/STT:4,/AirTrunk:2");
    expectDistribution=  migrationRuleMap.getRuleFromDistributionWithZS(distribution, replication,
        validDataCenters);
    Assert.assertEquals(ReplicationRule.parseFromString("/AirTrunk:2,/YTL:1,/STT:3"),
        expectDistribution);
  }
}
