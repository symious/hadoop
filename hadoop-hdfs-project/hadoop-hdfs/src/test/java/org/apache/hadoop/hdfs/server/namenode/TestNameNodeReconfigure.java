/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.Collection;
import java.util.SortedSet;

import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.zoneservice.ReplicationRule;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_SERVER_LOG_SLOW_RPC;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_SERVER_LOG_SLOW_RPC_THRESHOLD_MS_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_SERVER_LOG_SLOW_RPC_THRESHOLD_MS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_PEER_STATS_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_TAILEDITS_ONLY_DURABLE_TXNS_ENABLE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_TAILEDITS_ONLY_DURABLE_TXNS_ENABLE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_IMAGE_PARALLEL_LOAD_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACL_CONSTRAINTS_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACL_ALLOW_USERS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_ADD_BLOCKS_ENABLED;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AVOID_SLOW_DATANODE_FOR_READ_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BLOCKPLACEMENTPOLICY_EXCLUDE_SLOW_NODES_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_CREATE_SYMLNK_ALLOW_USERS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_CREATE_SYMLNK_CONSTRAINTS_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELETE_REDUNDANT_DATACENTERS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DISABLE_EC_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DR_COLD_DATA_THRESHOLD_MS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DR_COLD_DATA_THRESHOLD_MS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DR_DATACENTERS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DR_REPLICATION_RULE_COLD_DATA_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DR_REPLICATION_RULE_ENABLE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DR_STRIPED_BLOCK_RULE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_EXCESS_REDUNDANCY_TIMEOUT_CHECK_ENABLED;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_EXCESS_REDUNDANCY_TIMEOUT_CHECK_LIMIT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_EXCESS_REDUNDANCY_TIMEOUT_CHECK_LIMIT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_EXCESS_REDUNDANCY_TIMEOUT_SEC_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_EXCESS_REDUNDANCY_TIMEOUT_SEC_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LOCK_DETAILED_METRICS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_QUOTA_INIT_THREADS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_QUOTA_INIT_THREADS_MAXIMUM;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_FAULTY_DC_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SYMLINKS_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_WRITE_LOCK_REPORTING_THRESHOLD_MS_KEY;
import static org.junit.Assert.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.StoragePolicySatisfierMode;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.namenode.sps.StoragePolicySatisfyManager;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.test.GenericTestUtils;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_ENABLED_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_INVALIDATE_LIMIT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_MAX_SLOWPEER_COLLECT_NODES_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_BACKOFF_ENABLE_DEFAULT;

public class TestNameNodeReconfigure {

  public static final Logger LOG = LoggerFactory
      .getLogger(TestNameNodeReconfigure.class);

  private MiniDFSCluster cluster;
  private final int customizedBlockInvalidateLimit = 500;

  @Before
  public void setUp() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFS_BLOCK_INVALIDATE_LIMIT_KEY,
        customizedBlockInvalidateLimit);
    cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .build();
    cluster.waitActive();
    cluster.transitionToActive(0);
  }

  @Test
  public void testReconfigureCallerContextEnabled()
      throws ReconfigurationException {
    final NameNode nameNode = cluster.getNameNode(0);
    final FSNamesystem nameSystem = nameNode.getNamesystem();

    // try invalid values
    nameNode.reconfigureProperty(HADOOP_CALLER_CONTEXT_ENABLED_KEY, "text");
    verifyReconfigureCallerContextEnabled(nameNode, nameSystem, false);

    // enable CallerContext
    nameNode.reconfigureProperty(HADOOP_CALLER_CONTEXT_ENABLED_KEY, "true");
    verifyReconfigureCallerContextEnabled(nameNode, nameSystem, true);

    // disable CallerContext
    nameNode.reconfigureProperty(HADOOP_CALLER_CONTEXT_ENABLED_KEY, "false");
    verifyReconfigureCallerContextEnabled(nameNode, nameSystem, false);

    // revert to default
    nameNode.reconfigureProperty(HADOOP_CALLER_CONTEXT_ENABLED_KEY, null);

    // verify default
    assertEquals(HADOOP_CALLER_CONTEXT_ENABLED_KEY + " has wrong value", false,
        nameSystem.getCallerContextEnabled());
    assertEquals(HADOOP_CALLER_CONTEXT_ENABLED_KEY + " has wrong value", null,
        nameNode.getConf().get(HADOOP_CALLER_CONTEXT_ENABLED_KEY));
  }

  void verifyReconfigureCallerContextEnabled(final NameNode nameNode,
      final FSNamesystem nameSystem, boolean expected) {
    assertEquals(HADOOP_CALLER_CONTEXT_ENABLED_KEY + " has wrong value",
        expected, nameNode.getNamesystem().getCallerContextEnabled());
    assertEquals(
        HADOOP_CALLER_CONTEXT_ENABLED_KEY + " has wrong value",
        expected,
        nameNode.getConf().getBoolean(HADOOP_CALLER_CONTEXT_ENABLED_KEY,
            HADOOP_CALLER_CONTEXT_ENABLED_DEFAULT));
  }

  /**
   * Test to reconfigure enable/disable IPC backoff
   */
  @Test
  public void testReconfigureIPCBackoff() throws ReconfigurationException {
    final NameNode nameNode = cluster.getNameNode(0);
    NameNodeRpcServer nnrs = (NameNodeRpcServer) nameNode.getRpcServer();

    String ipcClientRPCBackoffEnable = NameNode.buildBackoffEnableKey(nnrs
        .getClientRpcServer().getPort());

    // try invalid values
    verifyReconfigureIPCBackoff(nameNode, nnrs, ipcClientRPCBackoffEnable,
        false);

    // enable IPC_CLIENT_RPC_BACKOFF
    nameNode.reconfigureProperty(ipcClientRPCBackoffEnable, "true");
    verifyReconfigureIPCBackoff(nameNode, nnrs, ipcClientRPCBackoffEnable,
        true);

    // disable IPC_CLIENT_RPC_BACKOFF
    nameNode.reconfigureProperty(ipcClientRPCBackoffEnable, "false");
    verifyReconfigureIPCBackoff(nameNode, nnrs, ipcClientRPCBackoffEnable,
        false);

    // revert to default
    nameNode.reconfigureProperty(ipcClientRPCBackoffEnable, null);
    assertEquals(ipcClientRPCBackoffEnable + " has wrong value", false,
        nnrs.getClientRpcServer().isClientBackoffEnabled());
    assertEquals(ipcClientRPCBackoffEnable + " has wrong value", null,
        nameNode.getConf().get(ipcClientRPCBackoffEnable));
  }

  void verifyReconfigureIPCBackoff(final NameNode nameNode,
      final NameNodeRpcServer nnrs, String property, boolean expected) {
    assertEquals(property + " has wrong value", expected, nnrs
        .getClientRpcServer().isClientBackoffEnabled());
    assertEquals(property + " has wrong value", expected, nameNode.getConf()
        .getBoolean(property, IPC_BACKOFF_ENABLE_DEFAULT));
  }

  /**
   * Test to reconfigure interval of heart beat check and re-check.
   */
  @Test
  public void testReconfigureHearbeatCheck() throws ReconfigurationException {
    final NameNode nameNode = cluster.getNameNode(0);
    final DatanodeManager datanodeManager = nameNode.namesystem
        .getBlockManager().getDatanodeManager();
    // change properties
    nameNode.reconfigureProperty(DFS_HEARTBEAT_INTERVAL_KEY, "" + 6);
    nameNode.reconfigureProperty(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
        "" + (10 * 60 * 1000));

    // try invalid values
    try {
      nameNode.reconfigureProperty(DFS_HEARTBEAT_INTERVAL_KEY, "text");
      fail("ReconfigurationException expected");
    } catch (ReconfigurationException expected) {
      assertTrue(expected.getCause() instanceof NumberFormatException);
    }
    try {
      nameNode.reconfigureProperty(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
          "text");
      fail("ReconfigurationException expected");
    } catch (ReconfigurationException expected) {
      assertTrue(expected.getCause() instanceof NumberFormatException);
    }

    // verify change
    assertEquals(
        DFS_HEARTBEAT_INTERVAL_KEY + " has wrong value",
        6,
        nameNode.getConf().getLong(DFS_HEARTBEAT_INTERVAL_KEY,
            DFS_HEARTBEAT_INTERVAL_DEFAULT));
    assertEquals(DFS_HEARTBEAT_INTERVAL_KEY + " has wrong value", 6,
        datanodeManager.getHeartbeatInterval());

    assertEquals(
        DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY + " has wrong value",
        10 * 60 * 1000,
        nameNode.getConf().getInt(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
            DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT));
    assertEquals(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY
        + " has wrong value", 10 * 60 * 1000,
        datanodeManager.getHeartbeatRecheckInterval());

    // change to a value with time unit
    nameNode.reconfigureProperty(DFS_HEARTBEAT_INTERVAL_KEY, "1m");

    assertEquals(
        DFS_HEARTBEAT_INTERVAL_KEY + " has wrong value",
        60,
        nameNode.getConf().getLong(DFS_HEARTBEAT_INTERVAL_KEY,
            DFS_HEARTBEAT_INTERVAL_DEFAULT));
    assertEquals(DFS_HEARTBEAT_INTERVAL_KEY + " has wrong value", 60,
        datanodeManager.getHeartbeatInterval());

    // revert to defaults
    nameNode.reconfigureProperty(DFS_HEARTBEAT_INTERVAL_KEY, null);
    nameNode.reconfigureProperty(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
        null);

    // verify defaults
    assertEquals(DFS_HEARTBEAT_INTERVAL_KEY + " has wrong value", null,
        nameNode.getConf().get(DFS_HEARTBEAT_INTERVAL_KEY));
    assertEquals(DFS_HEARTBEAT_INTERVAL_KEY + " has wrong value",
        DFS_HEARTBEAT_INTERVAL_DEFAULT, datanodeManager.getHeartbeatInterval());

    assertEquals(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY
        + " has wrong value", null,
        nameNode.getConf().get(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY));
    assertEquals(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY
        + " has wrong value", DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT,
        datanodeManager.getHeartbeatRecheckInterval());
  }

  /**
   * Tests enable/disable Storage Policy Satisfier dynamically when
   * "dfs.storage.policy.enabled" feature is disabled.
   *
   * @throws ReconfigurationException
   * @throws IOException
   */
  @Test(timeout = 30000)
  public void testReconfigureSPSWithStoragePolicyDisabled()
      throws ReconfigurationException, IOException {
    // shutdown cluster
    cluster.shutdown();
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_KEY, false);
    cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitActive();

    final NameNode nameNode = cluster.getNameNode();
    verifySPSEnabled(nameNode, DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        StoragePolicySatisfierMode.NONE, false);

    // enable SPS internally by keeping DFS_STORAGE_POLICY_ENABLED_KEY
    nameNode.reconfigureProperty(DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        StoragePolicySatisfierMode.EXTERNAL.toString());

    // Since DFS_STORAGE_POLICY_ENABLED_KEY is disabled, SPS can't be enabled.
    assertNull("SPS shouldn't start as "
        + DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_KEY + " is disabled",
            nameNode.getNamesystem().getBlockManager().getSPSManager());
    verifySPSEnabled(nameNode, DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        StoragePolicySatisfierMode.EXTERNAL, false);

    assertEquals(DFS_STORAGE_POLICY_SATISFIER_MODE_KEY + " has wrong value",
        StoragePolicySatisfierMode.EXTERNAL.toString(), nameNode.getConf()
            .get(DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
            DFS_STORAGE_POLICY_SATISFIER_MODE_DEFAULT));
  }

  /**
   * Tests enable/disable Storage Policy Satisfier dynamically.
   */
  @Test(timeout = 30000)
  public void testReconfigureStoragePolicySatisfierEnabled()
      throws ReconfigurationException {
    final NameNode nameNode = cluster.getNameNode(0);

    verifySPSEnabled(nameNode, DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        StoragePolicySatisfierMode.NONE, false);
    // try invalid values
    try {
      nameNode.reconfigureProperty(DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
          "text");
      fail("ReconfigurationException expected");
    } catch (ReconfigurationException e) {
      GenericTestUtils.assertExceptionContains(
          "For enabling or disabling storage policy satisfier, must "
              + "pass either internal/external/none string value only",
          e.getCause());
    }

    // disable SPS
    nameNode.reconfigureProperty(DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        StoragePolicySatisfierMode.NONE.toString());
    verifySPSEnabled(nameNode, DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        StoragePolicySatisfierMode.NONE, false);

    // enable external SPS
    nameNode.reconfigureProperty(DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        StoragePolicySatisfierMode.EXTERNAL.toString());
    assertEquals(DFS_STORAGE_POLICY_SATISFIER_MODE_KEY + " has wrong value",
        false, nameNode.getNamesystem().getBlockManager().getSPSManager()
            .isSatisfierRunning());
    assertEquals(DFS_STORAGE_POLICY_SATISFIER_MODE_KEY + " has wrong value",
        StoragePolicySatisfierMode.EXTERNAL.toString(),
        nameNode.getConf().get(DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
            DFS_STORAGE_POLICY_SATISFIER_MODE_DEFAULT));
  }

  /**
   * Test to satisfy storage policy after disabled storage policy satisfier.
   */
  @Test(timeout = 30000)
  public void testSatisfyStoragePolicyAfterSatisfierDisabled()
      throws ReconfigurationException, IOException {
    final NameNode nameNode = cluster.getNameNode(0);

    // disable SPS
    nameNode.reconfigureProperty(DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        StoragePolicySatisfierMode.NONE.toString());
    verifySPSEnabled(nameNode, DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        StoragePolicySatisfierMode.NONE, false);

    Path filePath = new Path("/testSPS");
    DistributedFileSystem fileSystem = cluster.getFileSystem(0);
    fileSystem.create(filePath);
    fileSystem.setStoragePolicy(filePath, "COLD");
    try {
      fileSystem.satisfyStoragePolicy(filePath);
      fail("Expected to fail, as storage policy feature has disabled.");
    } catch (RemoteException e) {
      GenericTestUtils
          .assertExceptionContains("Cannot request to satisfy storage policy "
              + "when storage policy satisfier feature has been disabled"
              + " by admin. Seek for an admin help to enable it "
              + "or use Mover tool.", e);
    }
  }

  void verifySPSEnabled(final NameNode nameNode, String property,
      StoragePolicySatisfierMode expected, boolean isSatisfierRunning) {
    StoragePolicySatisfyManager spsMgr = nameNode
            .getNamesystem().getBlockManager().getSPSManager();
    boolean isSPSRunning = spsMgr != null ? spsMgr.isSatisfierRunning()
        : false;
    assertEquals(property + " has wrong value", isSPSRunning, isSPSRunning);
    String actual = nameNode.getConf().get(property,
        DFS_STORAGE_POLICY_SATISFIER_MODE_DEFAULT);
    assertEquals(property + " has wrong value", expected,
        StoragePolicySatisfierMode.fromString(actual));
  }

  @Test
  public void testBlockInvalidateLimitAfterReconfigured()
      throws ReconfigurationException {
    final NameNode nameNode = cluster.getNameNode(0);
    final DatanodeManager datanodeManager = nameNode.namesystem
        .getBlockManager().getDatanodeManager();

    assertEquals(DFS_BLOCK_INVALIDATE_LIMIT_KEY + " is not correctly set",
        customizedBlockInvalidateLimit,
        datanodeManager.getBlockInvalidateLimit());

    nameNode.reconfigureProperty(DFS_HEARTBEAT_INTERVAL_KEY,
        Integer.toString(6));

    // 20 * 6 = 120 < 500
    // Invalid block limit should stay same as before after reconfiguration.
    assertEquals(DFS_BLOCK_INVALIDATE_LIMIT_KEY
            + " is not honored after reconfiguration",
        customizedBlockInvalidateLimit,
        datanodeManager.getBlockInvalidateLimit());

    nameNode.reconfigureProperty(DFS_HEARTBEAT_INTERVAL_KEY,
        Integer.toString(50));

    // 20 * 50 = 1000 > 500
    // Invalid block limit should be reset to 1000
    assertEquals(DFS_BLOCK_INVALIDATE_LIMIT_KEY
            + " is not reconfigured correctly",
        1000,
        datanodeManager.getBlockInvalidateLimit());
  }

  @Test
  public void testEnableParallelLoadAfterReconfigured()
      throws ReconfigurationException {
    final NameNode nameNode = cluster.getNameNode(0);

    // By default, enableParallelLoad is false
    assertEquals(false, FSImageFormatProtobuf.getEnableParallelLoad());

    nameNode.reconfigureProperty(DFS_IMAGE_PARALLEL_LOAD_KEY,
        Boolean.toString(true));

    // After reconfigured, enableParallelLoad is true
    assertEquals(true, FSImageFormatProtobuf.getEnableParallelLoad());
  }

  @Test
  public void testReconfigureTailEditsOnlyDurable() throws ReconfigurationException {
    NameNode nameNode = cluster.getNameNode(0);
    if (nameNode.getState().equals("active")) {
      assertTrue(nameNode.getConf().getBoolean(
          DFS_HA_TAILEDITS_ONLY_DURABLE_TXNS_ENABLE_KEY,
          DFS_HA_TAILEDITS_ONLY_DURABLE_TXNS_ENABLE_DEFAULT));
      nameNode.reconfigureProperty(DFS_HA_TAILEDITS_ONLY_DURABLE_TXNS_ENABLE_KEY,
          Boolean.toString(false));
      assertFalse(nameNode.getConf().getBoolean(
          DFS_HA_TAILEDITS_ONLY_DURABLE_TXNS_ENABLE_KEY,
          DFS_HA_TAILEDITS_ONLY_DURABLE_TXNS_ENABLE_DEFAULT));
      nameNode = cluster.getNameNode(1);
    }
    assertEquals("standby", nameNode.getState());
    FSNamesystem fsNamesystem = nameNode.getNamesystem();
    assertTrue(fsNamesystem.getEditLogTailer().isOnlyDurableTxns());

    nameNode.reconfigureProperty(DFS_HA_TAILEDITS_ONLY_DURABLE_TXNS_ENABLE_KEY,
        Boolean.toString(false));
    assertFalse(fsNamesystem.getEditLogTailer().isOnlyDurableTxns());
    assertFalse(nameNode.getConf().getBoolean(
        DFS_HA_TAILEDITS_ONLY_DURABLE_TXNS_ENABLE_KEY,
        DFS_HA_TAILEDITS_ONLY_DURABLE_TXNS_ENABLE_DEFAULT));

    nameNode.reconfigureProperty(DFS_HA_TAILEDITS_ONLY_DURABLE_TXNS_ENABLE_KEY,
        Boolean.toString(true));
    assertTrue(fsNamesystem.getEditLogTailer().isOnlyDurableTxns());
    assertTrue(nameNode.getConf().getBoolean(
        DFS_HA_TAILEDITS_ONLY_DURABLE_TXNS_ENABLE_KEY,
        DFS_HA_TAILEDITS_ONLY_DURABLE_TXNS_ENABLE_DEFAULT));
  }

  @Test
  public void testEnableSlowNodesParametersAfterReconfigured()
      throws ReconfigurationException {
    final NameNode nameNode = cluster.getNameNode(0);
    final BlockManager blockManager = nameNode.namesystem.getBlockManager();
    final DatanodeManager datanodeManager = blockManager.getDatanodeManager();

    // By default, avoidSlowDataNodesForRead is false.
    assertFalse(datanodeManager.getEnableAvoidSlowDataNodesForRead());
    nameNode.reconfigureProperty(
        DFS_NAMENODE_AVOID_SLOW_DATANODE_FOR_READ_KEY, Boolean.toString(true));

    // After reconfigured, avoidSlowDataNodesForRead is true.
    assertTrue(datanodeManager.getEnableAvoidSlowDataNodesForRead());

    // By default, excludeSlowNodesEnabled is false.
    assertFalse(blockManager.getExcludeSlowNodesEnabled(BlockType.CONTIGUOUS));
    assertFalse(blockManager.getExcludeSlowNodesEnabled(BlockType.STRIPED));

    nameNode.reconfigureProperty(
        DFS_NAMENODE_BLOCKPLACEMENTPOLICY_EXCLUDE_SLOW_NODES_ENABLED_KEY, Boolean.toString(true));

    // After reconfigured, excludeSlowNodesEnabled is true.
    assertTrue(blockManager.getExcludeSlowNodesEnabled(BlockType.CONTIGUOUS));
    assertTrue(blockManager.getExcludeSlowNodesEnabled(BlockType.STRIPED));
  }

  @Test
  public void testSlowPeerTrackerEnabled() throws Exception {
    final NameNode nameNode = cluster.getNameNode(0);
    final DatanodeManager datanodeManager = nameNode.namesystem.getBlockManager()
        .getDatanodeManager();

    assertFalse("SlowNode tracker is already enabled. It should be disabled by default",
        datanodeManager.getSlowPeerTracker().isSlowPeerTrackerEnabled());

    try {
      nameNode.reconfigurePropertyImpl(DFS_DATANODE_PEER_STATS_ENABLED_KEY, "non-boolean");
      fail("should not reach here");
    } catch (ReconfigurationException e) {
      assertEquals(
          "Could not change property dfs.datanode.peer.stats.enabled from 'false' to 'non-boolean'",
          e.getMessage());
    }

    nameNode.reconfigurePropertyImpl(DFS_DATANODE_PEER_STATS_ENABLED_KEY, "True");
    assertTrue("SlowNode tracker is still disabled. Reconfiguration could not be successful",
        datanodeManager.getSlowPeerTracker().isSlowPeerTrackerEnabled());

    nameNode.reconfigurePropertyImpl(DFS_DATANODE_PEER_STATS_ENABLED_KEY, null);
    assertFalse("SlowNode tracker is still enabled. Reconfiguration could not be successful",
        datanodeManager.getSlowPeerTracker().isSlowPeerTrackerEnabled());

  }

  @Test
  public void testReconfigureMaxSlowpeerCollectNodes()
      throws ReconfigurationException {
    final NameNode nameNode = cluster.getNameNode(0);
    final DatanodeManager datanodeManager = nameNode.namesystem
        .getBlockManager().getDatanodeManager();

    // By default, DFS_NAMENODE_MAX_SLOWPEER_COLLECT_NODES_KEY is 5.
    assertEquals(5, datanodeManager.getMaxSlowpeerCollectNodes());

    // Reconfigure.
    nameNode.reconfigureProperty(
        DFS_NAMENODE_MAX_SLOWPEER_COLLECT_NODES_KEY, Integer.toString(10));

    // Assert DFS_NAMENODE_MAX_SLOWPEER_COLLECT_NODES_KEY is 10.
    assertEquals(10, datanodeManager.getMaxSlowpeerCollectNodes());
  }

  @Test
  public void testReconfigureFSNamesystemLockMetricsParameters()
      throws ReconfigurationException, IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFS_NAMENODE_LOCK_DETAILED_METRICS_KEY, false);
    long defaultReadLockMS = 1000L;
    conf.setLong(DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_KEY, defaultReadLockMS);
    long defaultWriteLockMS = 1000L;
    conf.setLong(DFS_NAMENODE_WRITE_LOCK_REPORTING_THRESHOLD_MS_KEY, defaultWriteLockMS);

    try (MiniDFSCluster newCluster = new MiniDFSCluster.Builder(conf).build()) {
      newCluster.waitActive();
      final NameNode nameNode = newCluster.getNameNode();
      final FSNamesystem fsNamesystem = nameNode.getNamesystem();
      // verify default value.
      assertFalse(fsNamesystem.isMetricsEnabled());
      assertEquals(defaultReadLockMS, fsNamesystem.getReadLockReportingThresholdMs());
      assertEquals(defaultWriteLockMS, fsNamesystem.getWriteLockReportingThresholdMs());

      // try invalid metricsEnabled.
      try {
        nameNode.reconfigurePropertyImpl(DFS_NAMENODE_LOCK_DETAILED_METRICS_KEY,
            "non-boolean");
        fail("should not reach here");
      } catch (ReconfigurationException e) {
        assertEquals(
            "Could not change property dfs.namenode.lock.detailed-metrics.enabled from " +
                "'false' to 'non-boolean'", e.getMessage());
      }

      // try correct metricsEnabled.
      nameNode.reconfigurePropertyImpl(DFS_NAMENODE_LOCK_DETAILED_METRICS_KEY, "true");
      assertTrue(fsNamesystem.isMetricsEnabled());

      nameNode.reconfigurePropertyImpl(DFS_NAMENODE_LOCK_DETAILED_METRICS_KEY, null);
      assertFalse(fsNamesystem.isMetricsEnabled());

      // try invalid readLockMS.
      try {
        nameNode.reconfigureProperty(DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_KEY,
            "non-numeric");
        fail("Should not reach here");
      } catch (ReconfigurationException e) {
        assertEquals("Could not change property " +
            "dfs.namenode.read-lock-reporting-threshold-ms from '" +
            defaultReadLockMS + "' to 'non-numeric'", e.getMessage());
      }

      // try correct readLockMS.
      nameNode.reconfigureProperty(DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_KEY,
          "20000");
      assertEquals(fsNamesystem.getReadLockReportingThresholdMs(), 20000);


      // try invalid writeLockMS.
      try {
        nameNode.reconfigureProperty(
            DFS_NAMENODE_WRITE_LOCK_REPORTING_THRESHOLD_MS_KEY, "non-numeric");
        fail("Should not reach here");
      } catch (ReconfigurationException e) {
        assertEquals("Could not change property " +
            "dfs.namenode.write-lock-reporting-threshold-ms from '" +
            defaultWriteLockMS + "' to 'non-numeric'", e.getMessage());
      }

      // try correct writeLockMS.
      nameNode.reconfigureProperty(
          DFS_NAMENODE_WRITE_LOCK_REPORTING_THRESHOLD_MS_KEY, "100000");
      assertEquals(fsNamesystem.getWriteLockReportingThresholdMs(), 100000);
    }
  }

  @After
  public void shutDown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testReconfigureQuotaInitThread()
      throws ReconfigurationException {
    final NameNode nameNode = cluster.getNameNode(0);
    final int oldQuotaInit = nameNode.getNamesystem()
        .getFSDirectory().getQuotaInitThreads();
    final int newQuotaInit = oldQuotaInit + 10;

    nameNode.reconfigureProperty(DFS_NAMENODE_QUOTA_INIT_THREADS_KEY,
        Integer.toString(newQuotaInit));
    assertEquals(newQuotaInit, nameNode.getNamesystem()
        .getFSDirectory().getQuotaInitThreads());
    assertEquals(newQuotaInit, nameNode.getConf().getInt(
        DFS_NAMENODE_QUOTA_INIT_THREADS_KEY, -1));

    nameNode.reconfigureProperty(DFS_NAMENODE_QUOTA_INIT_THREADS_KEY,
        Integer.toString(oldQuotaInit));
    assertEquals(oldQuotaInit, nameNode.getNamesystem()
        .getFSDirectory().getQuotaInitThreads());
    assertEquals(oldQuotaInit, nameNode.getConf().getInt(
        DFS_NAMENODE_QUOTA_INIT_THREADS_KEY, -1));


    int maxQuotaInit = DFS_NAMENODE_QUOTA_INIT_THREADS_MAXIMUM + 100;
    nameNode.reconfigureProperty(DFS_NAMENODE_QUOTA_INIT_THREADS_KEY,
        Integer.toString(maxQuotaInit));
    assertEquals(DFS_NAMENODE_QUOTA_INIT_THREADS_MAXIMUM,
        nameNode.getNamesystem().getFSDirectory().getQuotaInitThreads());
    assertEquals(DFS_NAMENODE_QUOTA_INIT_THREADS_MAXIMUM,
        nameNode.getConf().getInt(
            DFS_NAMENODE_QUOTA_INIT_THREADS_KEY, -1));

    int abnormalQuotaInit = -1;
    int preOldQuotaInit = nameNode.getNamesystem()
        .getFSDirectory().getQuotaInitThreads();
    nameNode.reconfigureProperty(DFS_NAMENODE_QUOTA_INIT_THREADS_KEY,
        Integer.toString(abnormalQuotaInit));
    assertEquals(preOldQuotaInit, nameNode.getNamesystem()
        .getFSDirectory().getQuotaInitThreads());
    assertEquals(preOldQuotaInit, nameNode.getConf().getInt(
        DFS_NAMENODE_QUOTA_INIT_THREADS_KEY, -1));
  }

  @Test
  public void testReconfigureDisableECFeature()
      throws ReconfigurationException {
    final NameNode nameNode = cluster.getNameNode(0);
    NameNodeRpcServer rpcServer = (NameNodeRpcServer) nameNode.getRpcServer();
    assertFalse(rpcServer.isDisableECFeature());

    nameNode.reconfigureProperty(DFS_NAMENODE_DISABLE_EC_KEY, "true");
    assertTrue(rpcServer.isDisableECFeature());

    nameNode.reconfigureProperty(DFS_NAMENODE_DISABLE_EC_KEY, "false");
    assertFalse(rpcServer.isDisableECFeature());
  }

  @Test
  public void testReconfigureFaultyDC() throws ReconfigurationException {
    final NameNode nameNode = cluster.getNameNode(0);
    BlockManager blockManager = nameNode.getNamesystem().getBlockManager();
    assertNull(blockManager.getFaultyDC());

    String newDC = "STT";
    nameNode.reconfigureProperty(DFS_NAMENODE_FAULTY_DC_KEY, newDC);
    assertEquals(newDC, blockManager.getFaultyDC());

    nameNode.reconfigureProperty(DFS_NAMENODE_FAULTY_DC_KEY, null);
    assertNull(blockManager.getFaultyDC());
  }

  @Test
  public void testReconfigureAclVerifyParameters()
      throws ReconfigurationException {
    final NameNode nameNode = cluster.getNameNode(0);
    FSNamesystem fsNamesystem = nameNode.getNamesystem();
    assertFalse(fsNamesystem.isEnableAclConstraints());

    nameNode.reconfigureProperty(DFS_NAMENODE_ACL_CONSTRAINTS_ENABLED_KEY, "true");
    assertTrue(fsNamesystem.isEnableAclConstraints());

    nameNode.reconfigureProperty(DFS_NAMENODE_ACL_CONSTRAINTS_ENABLED_KEY, "false");
    assertFalse(fsNamesystem.isEnableAclConstraints());

    SortedSet<String> aclAllowUsers = fsNamesystem.getAclAllowUsers();
    assertEquals(0, aclAllowUsers.size());

    nameNode.reconfigureProperty(DFS_NAMENODE_ACL_ALLOW_USERS, "user1,user2");
    aclAllowUsers = fsNamesystem.getAclAllowUsers();
    assertEquals(2, aclAllowUsers.size());
    assertTrue(aclAllowUsers.contains("user1"));
    assertTrue(aclAllowUsers.contains("user2"));
    assertFalse(aclAllowUsers.contains("user3"));

    nameNode.reconfigureProperty(DFS_NAMENODE_ACL_ALLOW_USERS, "");
    aclAllowUsers = fsNamesystem.getAclAllowUsers();
    assertEquals(0, aclAllowUsers.size());
  }

  @Test
  public void testReconfigureCreateSymlinkVerifyParameters()
      throws ReconfigurationException {
    final NameNode nameNode = cluster.getNameNode(0);
    FSNamesystem fsNamesystem = nameNode.getNamesystem();
    assertFalse(fsNamesystem.isEnableCreateSymlinkConstraints());

    nameNode.reconfigureProperty(DFS_NAMENODE_CREATE_SYMLNK_CONSTRAINTS_ENABLED_KEY, "true");
    assertTrue(fsNamesystem.isEnableCreateSymlinkConstraints());

    nameNode.reconfigureProperty(DFS_NAMENODE_CREATE_SYMLNK_CONSTRAINTS_ENABLED_KEY, "false");
    assertFalse(fsNamesystem.isEnableCreateSymlinkConstraints());

    SortedSet<String> createSymlinkAllowUsers = fsNamesystem.getCreateSymlinkAllowUsers();
    assertEquals(0, createSymlinkAllowUsers.size());

    nameNode.reconfigureProperty(DFS_NAMENODE_CREATE_SYMLNK_ALLOW_USERS, "user1,user2");
    createSymlinkAllowUsers = fsNamesystem.getCreateSymlinkAllowUsers();
    assertEquals(2, createSymlinkAllowUsers.size());
    assertTrue(createSymlinkAllowUsers.contains("user1"));
    assertTrue(createSymlinkAllowUsers.contains("user2"));
    assertFalse(createSymlinkAllowUsers.contains("user3"));

    nameNode.reconfigureProperty(DFS_NAMENODE_CREATE_SYMLNK_ALLOW_USERS, "");
    createSymlinkAllowUsers = fsNamesystem.getCreateSymlinkAllowUsers();
    assertEquals(0, createSymlinkAllowUsers.size());
  }

  @Test
  public void testReconfigureDisableSymlinksFeature() throws ReconfigurationException {
    final NameNode nameNode = cluster.getNameNode(0);
    FSNamesystem fsNamesystem = nameNode.getNamesystem();
    // Enabling symlinks is enabled by default when create MiniDFSCluster.
    assertTrue(fsNamesystem.isEnableSymlinks());

    nameNode.reconfigureProperty(DFS_NAMENODE_SYMLINKS_ENABLED_KEY, "false");
    assertFalse(fsNamesystem.isEnableSymlinks());
  }

  @Test
  public void testReconfigureDelRedundantDataCenters() throws ReconfigurationException {
    final NameNode nameNode = cluster.getNameNode(0);
    final BlockManager bm = nameNode.getNamesystem().getBlockManager();
    Collection<String> delRedundantDataCenters = bm.getDelRedundantDataCenters();
    assertEquals(0, delRedundantDataCenters.size());

    nameNode.reconfigureProperty(DFS_NAMENODE_DELETE_REDUNDANT_DATACENTERS, "/TL,/AT");
    delRedundantDataCenters = bm.getDelRedundantDataCenters();
    assertEquals(2, delRedundantDataCenters.size());
    assertTrue(delRedundantDataCenters.contains("/TL"));
    assertTrue(delRedundantDataCenters.contains("/AT"));
    assertFalse(delRedundantDataCenters.contains("/STT"));

    nameNode.reconfigureProperty(DFS_NAMENODE_DELETE_REDUNDANT_DATACENTERS, "");
    delRedundantDataCenters = bm.getDelRedundantDataCenters();
    assertEquals(0, delRedundantDataCenters.size());
  }

  @Test
  public void testReconfigureMinBlocksForWrite() throws Exception {
    final NameNode nameNode = cluster.getNameNode(0);
    final BlockManager bm = nameNode.getNamesystem().getBlockManager();
    String key = DFSConfigKeys.DFS_NAMENODE_BLOCKPLACEMENTPOLICY_MIN_BLOCKS_FOR_WRITE_KEY;
    int defaultVal = DFSConfigKeys.DFS_NAMENODE_BLOCKPLACEMENTPOLICY_MIN_BLOCKS_FOR_WRITE_DEFAULT;

    // Ensure we cannot set any of the parameters negative
    ReconfigurationException reconfigurationException =
        LambdaTestUtils.intercept(ReconfigurationException.class,
            () -> nameNode.reconfigurePropertyImpl(key, "-20"));
    assertTrue(reconfigurationException.getCause() instanceof IllegalArgumentException);
    assertEquals(key+" = '-20' is invalid. It should be a "
        +"positive, non-zero integer value.", reconfigurationException.getCause().getMessage());

    // Ensure none of the values were updated from the defaults
    assertEquals(defaultVal, bm.getMinBlocksForWrite(BlockType.CONTIGUOUS));
    assertEquals(defaultVal, bm.getMinBlocksForWrite(BlockType.STRIPED));

    reconfigurationException = LambdaTestUtils.intercept(ReconfigurationException.class,
            () -> nameNode.reconfigurePropertyImpl(key, "0"));
    assertTrue(reconfigurationException.getCause() instanceof IllegalArgumentException);
    assertEquals(key+" = '0' is invalid. It should be a "
        +"positive, non-zero integer value.", reconfigurationException.getCause().getMessage());

    // Ensure none of the values were updated from the defaults
    assertEquals(defaultVal, bm.getMinBlocksForWrite(BlockType.CONTIGUOUS));
    assertEquals(defaultVal, bm.getMinBlocksForWrite(BlockType.STRIPED));


    // Ensure none of the parameters can be set to a string value
    reconfigurationException = LambdaTestUtils.intercept(ReconfigurationException.class,
            () -> nameNode.reconfigurePropertyImpl(key, "str"));
    assertTrue(reconfigurationException.getCause() instanceof NumberFormatException);

    // Ensure none of the values were updated from the defaults
    assertEquals(defaultVal, bm.getMinBlocksForWrite(BlockType.CONTIGUOUS));
    assertEquals(defaultVal, bm.getMinBlocksForWrite(BlockType.STRIPED));

    nameNode.reconfigurePropertyImpl(key, "3");

    // Ensure none of the values were updated from the new value.
    assertEquals(3, bm.getMinBlocksForWrite(BlockType.CONTIGUOUS));
    assertEquals(3, bm.getMinBlocksForWrite(BlockType.STRIPED));
  }

  @Test
  public void testReconfigureLogSlowRPC() throws ReconfigurationException {
    final NameNode nameNode = cluster.getNameNode(0);
    final NameNodeRpcServer nnrs = (NameNodeRpcServer) nameNode.getRpcServer();
    // verify default value.
    assertFalse(nnrs.getClientRpcServer().isLogSlowRPC());
    assertEquals(IPC_SERVER_LOG_SLOW_RPC_THRESHOLD_MS_DEFAULT,
        nnrs.getClientRpcServer().getLogSlowRPCThresholdTime());

    // try invalid logSlowRPC.
    try {
      nameNode.reconfigurePropertyImpl(IPC_SERVER_LOG_SLOW_RPC, "non-boolean");
      fail("should not reach here");
    } catch (ReconfigurationException e) {
      assertEquals(
          "Could not change property ipc.server.log.slow.rpc from 'false' to 'non-boolean'",
          e.getMessage());
    }

    // try correct logSlowRPC.
    nameNode.reconfigurePropertyImpl(IPC_SERVER_LOG_SLOW_RPC, "True");
    assertTrue(nnrs.getClientRpcServer().isLogSlowRPC());

    // revert to defaults.
    nameNode.reconfigurePropertyImpl(IPC_SERVER_LOG_SLOW_RPC, null);
    assertFalse(nnrs.getClientRpcServer().isLogSlowRPC());

    // try invalid logSlowRPCThresholdTime.
    try {
      nameNode.reconfigureProperty(IPC_SERVER_LOG_SLOW_RPC_THRESHOLD_MS_KEY, "non-numeric");
      fail("Should not reach here");
    } catch (ReconfigurationException e) {
      assertEquals("Could not change property " + "ipc.server.log.slow.rpc.threshold.ms from '"
          + IPC_SERVER_LOG_SLOW_RPC_THRESHOLD_MS_DEFAULT + "' to 'non-numeric'", e.getMessage());
    }

    // try correct logSlowRPCThresholdTime.
    nameNode.reconfigureProperty(IPC_SERVER_LOG_SLOW_RPC_THRESHOLD_MS_KEY,
        "20000");
    assertEquals(nnrs.getClientRpcServer().getLogSlowRPCThresholdTime(), 20000);
  }

  @Test
  public void testReconfigureExcessRedundancyTimeoutCheckParameters()
      throws ReconfigurationException {
    final NameNode nameNode = cluster.getNameNode(0);
    final BlockManager bm = nameNode.getNamesystem().getBlockManager();
    // verify default value.
    assertFalse(bm.isExcessRedundancyTimeoutCheckEnabled());

    // try correct value.
    nameNode.reconfigurePropertyImpl(DFS_NAMENODE_EXCESS_REDUNDANCY_TIMEOUT_CHECK_ENABLED,
        "True");
    assertTrue(bm.isExcessRedundancyTimeoutCheckEnabled());

    // revert to defaults.
    nameNode.reconfigurePropertyImpl(DFS_NAMENODE_EXCESS_REDUNDANCY_TIMEOUT_CHECK_ENABLED,
        null);
    assertFalse(bm.isExcessRedundancyTimeoutCheckEnabled());

    // try invalid excessRedundancyTimeoutCheckLimit.
    try {
      nameNode.reconfigureProperty(DFS_NAMENODE_EXCESS_REDUNDANCY_TIMEOUT_CHECK_LIMIT,
          "non-numeric");
      fail("Should not reach here");
    } catch (ReconfigurationException e) {
      assertEquals("Could not change property " +
          "dfs.namenode.excess.redundancy.timeout.check.limit from '"
          + DFS_NAMENODE_EXCESS_REDUNDANCY_TIMEOUT_CHECK_LIMIT_DEFAULT
          + "' to 'non-numeric'", e.getMessage());
    }

    // try correct excessRedundancyTimeoutCheckLimit.
    nameNode.reconfigureProperty(DFS_NAMENODE_EXCESS_REDUNDANCY_TIMEOUT_CHECK_LIMIT,
        "20000");
    assertEquals(bm.getExcessRedundancyTimeoutCheckLimit(), 20000);

    // try invalid excessRedundancyTimeout.
    try {
      nameNode.reconfigureProperty(DFS_NAMENODE_EXCESS_REDUNDANCY_TIMEOUT_SEC_KEY,
          "non-numeric");
      fail("Should not reach here");
    } catch (ReconfigurationException e) {
      assertEquals("Could not change property " +
          "dfs.namenode.excess.redundancy.timeout-sec from '"
          + DFS_NAMENODE_EXCESS_REDUNDANCY_TIMEOUT_SEC_DEFAULT
          + "' to 'non-numeric'", e.getMessage());
    }

    // try correct excessRedundancyTimeout.
    nameNode.reconfigureProperty(DFS_NAMENODE_EXCESS_REDUNDANCY_TIMEOUT_SEC_KEY,
        "100");
    assertEquals(bm.getExcessRedundancyTimeout(), 100 * 1000);
  }

  @Test
  public void testReconfigureAuditLogAddBlocksEnable()
      throws ReconfigurationException {
    final NameNode nameNode = cluster.getNameNode(0);
    final DatanodeManager dm = nameNode.namesystem.getBlockManager().getDatanodeManager();
    // verify default value.
    assertFalse(dm.isEnableAuditLogAddBlocks());

    // try correct value.
    nameNode.reconfigurePropertyImpl(DFS_NAMENODE_AUDIT_LOG_ADD_BLOCKS_ENABLED,
        "True");
    assertTrue(dm.isEnableAuditLogAddBlocks());

    // revert to defaults.
    nameNode.reconfigurePropertyImpl(DFS_NAMENODE_AUDIT_LOG_ADD_BLOCKS_ENABLED,
        null);
    assertFalse(dm.isEnableAuditLogAddBlocks());
  }

  @Test
  public void testReconfigureAccessTimePrecision()
      throws ReconfigurationException {
    final NameNode nameNode = cluster.getNameNode(0);
    final FSDirectory fsDirectory = nameNode.namesystem.getFSDirectory();
    // verify default value.
    assertEquals(fsDirectory.getAccessTimePrecision(), DFS_NAMENODE_ACCESSTIME_PRECISION_DEFAULT);

    // try correct value.
    nameNode.reconfigurePropertyImpl(DFS_NAMENODE_ACCESSTIME_PRECISION_KEY, "0");
    assertEquals(fsDirectory.getAccessTimePrecision(), 0);

    nameNode.reconfigurePropertyImpl(DFS_NAMENODE_ACCESSTIME_PRECISION_KEY, "2000");
    assertEquals(fsDirectory.getAccessTimePrecision(), 2000);
  }

  @Test
  public void testReconfigureDRParameters()
      throws ReconfigurationException {
    final NameNode nameNode = cluster.getNameNode(0);
    final BlockManager bm = nameNode.getNamesystem().getBlockManager();
    // verify default value.
    assertFalse(bm.isDrReplicationRuleEnabled());

    // try correct value.
    nameNode.reconfigurePropertyImpl(DFS_NAMENODE_DR_REPLICATION_RULE_ENABLE_KEY,
        "True");
    assertTrue(bm.isDrReplicationRuleEnabled());

    // revert to defaults.
    nameNode.reconfigurePropertyImpl(DFS_NAMENODE_DR_REPLICATION_RULE_ENABLE_KEY,
        null);
    assertFalse(bm.isDrReplicationRuleEnabled());

    // try invalid drColdDataThresholdMS.
    try {
      nameNode.reconfigureProperty(DFS_NAMENODE_DR_COLD_DATA_THRESHOLD_MS_KEY,
          "non-numeric");
      fail("Should not reach here");
    } catch (ReconfigurationException e) {
      assertEquals("Could not change property " +
          "dfs.namenode.dr.cold-data.threshold.ms from '"
          + DFS_NAMENODE_DR_COLD_DATA_THRESHOLD_MS_DEFAULT
          + "' to 'non-numeric'", e.getMessage());
    }

    // try correct drColdDataThresholdMS.
    nameNode.reconfigureProperty(DFS_NAMENODE_DR_COLD_DATA_THRESHOLD_MS_KEY,
        "20000");
    assertEquals(bm.getDrColdDataThresholdMS(), 20000);

    nameNode.reconfigurePropertyImpl(DFS_NAMENODE_DR_REPLICATION_RULE_ENABLE_KEY,
        "True");
    assertTrue(bm.isDrReplicationRuleEnabled());

    // try correct drDataCenters.
    nameNode.reconfigureProperty(DFS_NAMENODE_DR_DATACENTERS_KEY,
        "/DC1,/DC2");
    assertEquals(2, bm.getDrDataCenters().size());
    assertTrue(bm.getDrDataCenters().contains("/DC1"));
    assertTrue(bm.getDrDataCenters().contains("/DC2"));

    // try invalid drDataCenters.
    try {
      nameNode.reconfigureProperty(DFS_NAMENODE_DR_DATACENTERS_KEY,
          "");
      fail("Should not reach here");
    } catch (ReconfigurationException e) {
      assertEquals("Could not change property " +
          "dfs.namenode.dr.datacenters from '/DC1,/DC2' to ''", e.getMessage());
    }

    // try correct drReplicationRuleForColdData.
    nameNode.reconfigureProperty(DFS_NAMENODE_DR_REPLICATION_RULE_COLD_DATA_KEY,
        "3=/DC1:1,/DC2:2");
    assertEquals(1, bm.getDrReplicationRuleForColdData().size());
    assertTrue(bm.getDrReplicationRuleForColdData().containsKey((short) 3));
    assertEquals(ReplicationRule.parseFromString("/DC1:1,/DC2:2"),
        bm.getDrReplicationRuleForColdData().get((short) 3));

    // try invalid drReplicationRuleForColdData.
    try {
      nameNode.reconfigureProperty(DFS_NAMENODE_DR_REPLICATION_RULE_COLD_DATA_KEY,
          "");
      fail("Should not reach here");
    } catch (ReconfigurationException e) {
      assertEquals("Could not change property " +
          "dfs.namenode.dr.replication-rule.cold-data from '3=/DC1:1,/DC2:2' " +
          "to ''", e.getMessage());
    }

    try {
      nameNode.reconfigureProperty(DFS_NAMENODE_DR_REPLICATION_RULE_COLD_DATA_KEY,
          "3=/DC1:1,/DC3:2");
      fail("Should not reach here");
    } catch (ReconfigurationException e) {
      assertEquals("Could not change property " +
          "dfs.namenode.dr.replication-rule.cold-data from '3=/DC1:1,/DC2:2' " +
          "to '3=/DC1:1,/DC3:2'", e.getMessage());
    }

    // try correct drStripedBlockRule.
    nameNode.reconfigureProperty(DFS_NAMENODE_DR_STRIPED_BLOCK_RULE_KEY,
        "9=/DC1:6,/DC2:3");
    assertEquals(1, bm.getDrStripedBlockRule().size());
    assertTrue(bm.getDrStripedBlockRule().containsKey((short) 9));
    assertEquals(ReplicationRule.parseFromString("/DC1:6,/DC2:3"),
        bm.getDrStripedBlockRule().get((short) 9));

    // try invalid drStripedBlockRule.
    try {
      nameNode.reconfigureProperty(DFS_NAMENODE_DR_STRIPED_BLOCK_RULE_KEY,
          "");
      fail("Should not reach here");
    } catch (ReconfigurationException e) {
      assertEquals("Could not change property " +
          "dfs.namenode.dr.striped.block-rule from '9=/DC1:6,/DC2:3' " +
          "to ''", e.getMessage());
    }

    try {
      nameNode.reconfigureProperty(DFS_NAMENODE_DR_STRIPED_BLOCK_RULE_KEY,
          "9=/DC1:6,/DC3:3");
      fail("Should not reach here");
    } catch (ReconfigurationException e) {
      assertEquals("Could not change property " +
          "dfs.namenode.dr.striped.block-rule from '9=/DC1:6,/DC2:3' " +
          "to '9=/DC1:6,/DC3:3'", e.getMessage());
    }

    // Update drDataCenters and will reset drReplicationRuleForColdData.
    nameNode.reconfigureProperty(DFS_NAMENODE_DR_DATACENTERS_KEY,
        "/DC1,/DC3");
    assertEquals(2, bm.getDrDataCenters().size());
    assertTrue(bm.getDrDataCenters().contains("/DC1"));
    assertTrue(bm.getDrDataCenters().contains("/DC3"));
    assertEquals(0, bm.getDrReplicationRuleForColdData().size());
    assertEquals(0, bm.getDrStripedBlockRule().size());

    // Update drReplicationRuleForColdData.
    nameNode.reconfigureProperty(DFS_NAMENODE_DR_REPLICATION_RULE_COLD_DATA_KEY,
        "3=/DC1:1,/DC3:2");
    assertEquals(1, bm.getDrReplicationRuleForColdData().size());
    assertTrue(bm.getDrReplicationRuleForColdData().containsKey((short) 3));
    assertEquals(ReplicationRule.parseFromString("/DC1:1,/DC3:2"),
        bm.getDrReplicationRuleForColdData().get((short) 3));

    // Update drStripedBlockRule.
    nameNode.reconfigureProperty(DFS_NAMENODE_DR_STRIPED_BLOCK_RULE_KEY,
        "9=/DC1:3,/DC3:6;14=/DC1:10,/DC3:4");
    assertEquals(2, bm.getDrStripedBlockRule().size());
    assertTrue(bm.getDrStripedBlockRule().containsKey((short) 9));
    assertTrue(bm.getDrStripedBlockRule().containsKey((short) 14));
    assertEquals(ReplicationRule.parseFromString("/DC1:3,/DC3:6"),
        bm.getDrStripedBlockRule().get((short) 9));
    assertEquals(ReplicationRule.parseFromString("/DC1:10,/DC3:4"),
        bm.getDrStripedBlockRule().get((short) 14));
  }

  @Test
  public void testReconfigureEnableFaultyDC()
      throws ReconfigurationException {
    final NameNode nameNode = cluster.getNameNode(0);
    final BlockManager blockManager = nameNode.namesystem.getBlockManager();
    // verify default value.
    assertEquals(DFSConfigKeys.DFS_NAMENODE_ENABLE_FAULTY_DC_MONITOR_DEFAULT,
        blockManager.getEnableFaultyDCMonitor());

    // try correct value.
    nameNode.reconfigurePropertyImpl(DFSConfigKeys.DFS_NAMENODE_ENABLE_FAULTY_DC_MONITOR_KEY, "true");
    assertTrue(blockManager.getEnableFaultyDCMonitor());

    nameNode.reconfigurePropertyImpl(DFSConfigKeys.DFS_NAMENODE_ENABLE_FAULTY_DC_MONITOR_KEY, "false");
    assertFalse(blockManager.getEnableFaultyDCMonitor());
  }

}
