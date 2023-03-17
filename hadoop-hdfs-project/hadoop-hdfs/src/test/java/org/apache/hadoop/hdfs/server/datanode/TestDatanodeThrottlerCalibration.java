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
package org.apache.hadoop.hdfs.server.datanode;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.hdfs.server.throttler.ThrottlerCalibrationMasterPolicyNoCalibration;
import org.apache.hadoop.hdfs.server.throttler.ThrottlerCalibrationSlavePolicy;
import org.apache.hadoop.hdfs.server.throttler.ThrottlerCalibrationMasterPolicy;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.throttler.ThrottlerCalibrationMasterPolicyAverageActiveNodes;
import org.apache.hadoop.hdfs.server.throttler.ThrottlerCalibrationSlavePolicyFirstComeFirstServed;
import org.apache.hadoop.hdfs.server.throttler.ThrottlerCalibrationSlavePolicyMedian;
import org.apache.hadoop.hdfs.server.throttler.ThrottlerCalibrationSlavePolicyNoCalibration;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.FakeTimer;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestDatanodeThrottlerCalibration {
  private static final Logger LOG = LoggerFactory.getLogger(TestDatanodeThrottlerCalibration.class);

  private static final int N_DNS = 10;
  private static final int N_NNS = 5;
  private static final long MASTER_BANDWIDTH = 500000;
  private static final long MINIMUM_BANDWIDTH = 200;

  private final DataNode[] dns = new DataNode[N_DNS];
  private final DummyNN[] nns = new DummyNN[N_NNS];

  // Cache because mockito can take up a lot of time
  private final ConcurrentMap<DataNode, ThrottlerCalibrationSlavePolicy> cachedSlavePolicies =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<DataNode, DataXceiverServer> cachedXferServers =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<DataNode, String> cachedDNUuids = new ConcurrentHashMap<>();

  @BeforeClass
  public static void disableLogging() {
    GenericTestUtils.setLogLevel(DataNode.LOG, Level.WARN);
  }

  private void setupMocks(Class<? extends ThrottlerCalibrationMasterPolicy> masterClass,
      Class<? extends ThrottlerCalibrationSlavePolicy> slaveClass, Configuration override,
      FakeTimer fakeTimer) {
    cachedSlavePolicies.clear();
    cachedXferServers.clear();
    cachedDNUuids.clear();

    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_DATANODE_DATA_READ_BANDWIDTHPERSEC_KEY, 1024);
    conf.setLong(DFSConfigKeys.DFS_DATANODE_DATA_WRITE_BANDWIDTHPERSEC_KEY, 1024);
    conf.setLong(DFSConfigKeys.DFS_DATANODE_DATA_TRANSFER_BANDWIDTHPERSEC_KEY, 1024);
    conf.setLong(DFSConfigKeys.DFS_THROTTLER_READ_MASTER_BANDWIDTH_KEY, MASTER_BANDWIDTH);
    conf.setLong(DFSConfigKeys.DFS_THROTTLER_WRITE_MASTER_BANDWIDTH_KEY, MASTER_BANDWIDTH);
    conf.setLong(DFSConfigKeys.DFS_THROTTLER_TRANSFER_MASTER_BANDWIDTH_KEY, MASTER_BANDWIDTH);
    conf.setTimeDuration(DFSConfigKeys.DFS_THROTTLER_AVERAGE_ACTIVE_POLICY_CALIBRATION_INTERVAL_KEY,
        1, TimeUnit.DAYS);
    conf.setTimeDuration(
        DFSConfigKeys.DFS_THROTTLER_FIRST_COME_FIRST_SERVED_POLICY_CALIBRATION_INTERVAL_KEY, 0,
        TimeUnit.MILLISECONDS);
    conf.setLong(DFSConfigKeys.DFS_THROTTLER_AVERAGE_ACTIVE_POLICY_MINIMUM_SLAVE_BANDWIDTH_KEY,
        MINIMUM_BANDWIDTH);
    conf.setLong(DFSConfigKeys.DFS_THROTTLER_AVERAGE_ACTIVE_POLICY_ACTIVE_THRESHOLD_KEY, 0);
    conf.setTimeDuration(DFSConfigKeys.DFS_THROTTLER_AVERAGE_ACTIVE_POLICY_GRACE_PERIOD_KEY, 1,
        TimeUnit.DAYS); // Master policy will be triggered manually during testing
    conf.set(DFSConfigKeys.DFS_THROTTLER_CALIBRATION_MASTER_POLICY_CLASSNAME_KEY,
        masterClass.getName());
    conf.set(DFSConfigKeys.DFS_THROTTLER_CALIBRATION_SLAVE_POLICY_CLASSNAME_KEY,
        slaveClass.getName());

    if (override != null) {
      override.addResource(conf);
      conf = override;
    }

    for (int i = 0; i < N_DNS; i++) {
      dns[i] = setupDNMock(conf, fakeTimer);
    }

    for (int i = 0; i < N_NNS; i++) {
      nns[i] = setupNNMock(conf, fakeTimer);
      for (int j = 0; j < N_DNS; j++) {
        nns[i].sendHeartbeat(dns[j]);
      }
    }
  }

  private DataNode setupDNMock(Configuration conf, FakeTimer fakeTimer) {
    DataNode mockDn = Mockito.mock(DataNode.class);
    String randomUUID = UUID.randomUUID().toString();
    mockDn.metrics = DataNodeMetrics.create(conf, randomUUID, null);
    Mockito.doReturn(randomUUID).when(mockDn).getDatanodeUuid();
    final DataXceiverServer xServer = new DataXceiverServer(null, conf, mockDn);
    Mockito.doReturn(xServer.getThrottlerCalibrationSlavePolicy()).when(mockDn)
        .getThrottlerCalibrationSlavePolicy();
    if (fakeTimer != null) {
      try {
        xServer.getThrottlerCalibrationSlavePolicy().setTimer(fakeTimer);
      } catch (Exception ignored) {
      }
    }
    Mockito.doReturn(xServer).when(mockDn).getXferServer();
    cachedSlavePolicies.put(mockDn, xServer.getThrottlerCalibrationSlavePolicy());
    cachedXferServers.put(mockDn, xServer);
    cachedDNUuids.put(mockDn, randomUUID);
    return mockDn;
  }

  private DummyNN setupNNMock(Configuration conf, FakeTimer fakeTimer) {
    DummyNN nn = new DummyNN(conf, fakeTimer);
    return nn;
  }

  /**
   * Simple dummy NN that can accept/pretend to fail to accept a DN heartbeat
   */
  class DummyNN {
    private final InetSocketAddress address;
    ThrottlerCalibrationMasterPolicy policy;

    DummyNN(Configuration conf, FakeTimer fakeTimer) {
      this.address = new InetSocketAddress(new Random().nextInt(65535));
      policy = ThrottlerCalibrationMasterPolicy.newThrottlerCalibrationPolicy(conf, null);
      if (fakeTimer != null) {
        try {
          policy.setTimer(fakeTimer);
        } catch (Exception ignored) {
        }
      }
    }

    private ConcurrentMap<String, DatanodeDescriptor> dns = new ConcurrentHashMap<>();

    public List<DatanodeDescriptor> getDNs() {
      return new ArrayList<>(dns.values());
    }

    public DatanodeDescriptor getOrAddDN(DatanodeRegistration toGet) {
      if (!dns.containsKey(toGet.getDatanodeUuid())) {
        dns.put(toGet.getDatanodeUuid(), new DatanodeDescriptor(toGet));
      }
      return dns.get(toGet.getDatanodeUuid());
    }

    public void sendHeartbeat(DataNode dn, double failureRate) {
      DatanodeRegistration datanodeRegistration = new DatanodeRegistration(
          new DatanodeID("127.0.0.1", "127.0.0.1", cachedDNUuids.get(dn), 12345, 12345, 12345,
              12345), new StorageInfo(HdfsServerConstants.NodeType.DATA_NODE),
          new ExportedBlockKeys(), "test");
      getOrAddDN(datanodeRegistration);
      ThrottlerCalibrationSlavePolicy throttlerManager = cachedSlavePolicies.get(dn);
      long readBytesThrottled =
          throttlerManager == null ? 0 : throttlerManager.getReadBytesThrottled();
      long writeBytesThrottled =
          throttlerManager == null ? 0 : throttlerManager.getWriteBytesThrottled();
      long transferBytesThrottled =
          throttlerManager == null ? 0 : throttlerManager.getTransferBytesThrottled();
      long[] newBandwidths =
          policy.getNewBandwidths(datanodeRegistration, readBytesThrottled, writeBytesThrottled,
              transferBytesThrottled);

      if (newBandwidths[0] == 0 && newBandwidths[1] == 0 && newBandwidths[2] == 0) {
        return;
      }
      if (new Random().nextDouble() < failureRate) {
        return;
      }
      cachedXferServers.get(dn)
          .calibrateThrottlers(this.address, newBandwidths[0], newBandwidths[1], newBandwidths[2]);
    }

    public void sendHeartbeat(DataNode dn) {
      sendHeartbeat(dn, 0);
    }
  }

  @Test
  public void testSimpleValues() {
    Configuration override = new Configuration();
    FakeTimer timer = new FakeTimer();
    timer.advance(40000000);

    override.setLong(DFSConfigKeys.DFS_DATANODE_DATA_READ_BANDWIDTHPERSEC_KEY, 4200);
    override.setLong(DFSConfigKeys.DFS_DATANODE_DATA_WRITE_BANDWIDTHPERSEC_KEY, 4200);
    override.setLong(DFSConfigKeys.DFS_DATANODE_DATA_TRANSFER_BANDWIDTHPERSEC_KEY, 4200);
    setupMocks(ThrottlerCalibrationMasterPolicyNoCalibration.class,
        ThrottlerCalibrationSlavePolicyFirstComeFirstServed.class, override, timer);

    DataXceiverServer dn = cachedXferServers.get(dns[0]);
    Assert.assertEquals(4200, dn.getReadThrottler().getBandwidth());
    Assert.assertEquals(4200, dn.getWriteThrottler().getBandwidth());
    Assert.assertEquals(4200, dn.getTransferThrottler().getBandwidth());

    // Invalid values
    timer.advance(1);
    dn.calibrateThrottlers(null, -1, -1, -1);
    Assert.assertEquals(4200, dn.getReadThrottler().getBandwidth());
    Assert.assertEquals(4200, dn.getWriteThrottler().getBandwidth());
    Assert.assertEquals(4200, dn.getTransferThrottler().getBandwidth());
    timer.advance(1);
    dn.calibrateThrottlers(null, -1, 50000, 50000);
    Assert.assertEquals(4200, dn.getReadThrottler().getBandwidth());
    Assert.assertEquals(4200, dn.getWriteThrottler().getBandwidth());
    Assert.assertEquals(4200, dn.getTransferThrottler().getBandwidth());

    // Update new values
    timer.advance(1);
    dn.calibrateThrottlers(null, 50000, 50000, 50000);
    Assert.assertEquals(50000, dn.getReadThrottler().getBandwidth());
    Assert.assertEquals(50000, dn.getWriteThrottler().getBandwidth());
    Assert.assertEquals(50000, dn.getTransferThrottler().getBandwidth());

    // Ignore all
    timer.advance(1);
    dn.calibrateThrottlers(null, 0, 0, 0);
    Assert.assertEquals(50000, dn.getReadThrottler().getBandwidth());
    Assert.assertEquals(50000, dn.getWriteThrottler().getBandwidth());
    Assert.assertEquals(50000, dn.getTransferThrottler().getBandwidth());

    // Ignore two
    timer.advance(1);
    dn.calibrateThrottlers(null, 0, 0, 4200);
    Assert.assertEquals(50000, dn.getReadThrottler().getBandwidth());
    Assert.assertEquals(50000, dn.getWriteThrottler().getBandwidth());
    Assert.assertEquals(4200, dn.getTransferThrottler().getBandwidth());

    // Ignore one
    timer.advance(1);
    dn.calibrateThrottlers(null, 3000, 0, 3000);
    Assert.assertEquals(3000, dn.getReadThrottler().getBandwidth());
    Assert.assertEquals(50000, dn.getWriteThrottler().getBandwidth());
    Assert.assertEquals(3000, dn.getTransferThrottler().getBandwidth());
  }

  @Test
  public void testSameBandwidthAllHeartbeats() {
    setupMocks(ThrottlerCalibrationMasterPolicyAverageActiveNodes.class,
        ThrottlerCalibrationSlavePolicyFirstComeFirstServed.class, null, null);
    // Read/Write/Transfer 100 bytes on all DNs
    // Do twice to stabilize bandwidths
    for (int count = 0; count < 2; count++) {
      for (int dnIdx = 0; dnIdx < N_DNS; dnIdx++) {
        cachedXferServers.get(dns[dnIdx]).getReadThrottler().dummyThrottleForTesting(100);
        cachedXferServers.get(dns[dnIdx]).getWriteThrottler().dummyThrottleForTesting(100);
        cachedXferServers.get(dns[dnIdx]).getTransferThrottler().dummyThrottleForTesting(100);
        for (int nnIdx = 0; nnIdx < N_NNS; nnIdx++) {
          nns[nnIdx].sendHeartbeat(dns[dnIdx]);
        }
      }
      for (int nnIdx = 0; nnIdx < N_NNS; nnIdx++) {
        ((ThrottlerCalibrationMasterPolicyAverageActiveNodes) nns[nnIdx].policy).triggerCalibrationForTesting();
      }
    }

    // Equal bandwidth on all DNs
    for (int dnIdx = 0; dnIdx < N_DNS; dnIdx++) {
      assertBandwidth(MASTER_BANDWIDTH / N_DNS, dnIdx);
    }
  }

  @Test
  public void testSomeNodesInactiveAllHeartbeats() {
    int lowTrafficNodes = 3;
    long lowTrafficThreshold = 100;

    FakeTimer timer = new FakeTimer();
    timer.advance(100000);
    Configuration override = new Configuration();
    override.setLong(DFSConfigKeys.DFS_THROTTLER_AVERAGE_ACTIVE_POLICY_ACTIVE_THRESHOLD_KEY,
        lowTrafficThreshold);

    setupMocks(ThrottlerCalibrationMasterPolicyAverageActiveNodes.class,
        ThrottlerCalibrationSlavePolicyFirstComeFirstServed.class, override, timer);

    // Low traffic nodes don't reach the threshold to be counted into master bandwidth
    for (int count = 0; count < 2; count++) {
      for (int dnIdx = 0; dnIdx < N_DNS; dnIdx++) {
        cachedXferServers.get(dns[dnIdx]).getReadThrottler()
            .dummyThrottleForTesting(dnIdx >= lowTrafficNodes ? lowTrafficThreshold * 10 : 10);
        cachedXferServers.get(dns[dnIdx]).getWriteThrottler()
            .dummyThrottleForTesting(dnIdx >= lowTrafficNodes ? lowTrafficThreshold * 10 : 10);
        cachedXferServers.get(dns[dnIdx]).getTransferThrottler()
            .dummyThrottleForTesting(dnIdx >= lowTrafficNodes ? lowTrafficThreshold * 10 : 10);
        timer.advance(1);
        for (int nnIdx = 0; nnIdx < N_NNS; nnIdx++) {
          nns[nnIdx].sendHeartbeat(dns[dnIdx]);
          timer.advance(1);
        }
      }
      for (int nnIdx = 0; nnIdx < N_NNS; nnIdx++) {
        ((ThrottlerCalibrationMasterPolicyAverageActiveNodes) nns[nnIdx].policy).triggerCalibrationForTesting();
      }
    }

    // Equal bandwidth on all DNs with traffic
    for (int dnIdx = lowTrafficNodes; dnIdx < N_DNS; dnIdx++) {
      assertBandwidth(MASTER_BANDWIDTH / (N_DNS - lowTrafficNodes), dnIdx);
    }
    // Min bandwidth for idle nodes
    for (int dnIdx = 0; dnIdx < lowTrafficNodes; dnIdx++) {
      assertBandwidth(MINIMUM_BANDWIDTH, dnIdx);
    }
  }

  @Test
  public void testAllNodesActiveDifferentSuggestions() {
    Configuration override = new Configuration();
    override.setTimeDuration(
        DFSConfigKeys.DFS_THROTTLER_FIRST_COME_FIRST_SERVED_POLICY_CALIBRATION_INTERVAL_KEY, 1,
        TimeUnit.SECONDS);
    FakeTimer timer = new FakeTimer();
    timer.advance((long) 1E9);

    {
      // This test needs at least 3 NNs
      assert N_NNS > 2;

      setupMocks(ThrottlerCalibrationMasterPolicyAverageActiveNodes.class,
          ThrottlerCalibrationSlavePolicyFirstComeFirstServed.class, override, timer);

      for (int count = 0; count < 2; count++) {
        for (int dnIdx = 0; dnIdx < N_DNS; dnIdx++) {
          cachedXferServers.get(dns[dnIdx]).getReadThrottler().dummyThrottleForTesting(1000);
          cachedXferServers.get(dns[dnIdx]).getWriteThrottler().dummyThrottleForTesting(1000);
          cachedXferServers.get(dns[dnIdx]).getTransferThrottler().dummyThrottleForTesting(1000);
          for (int nnIdx = 0; nnIdx < N_NNS; nnIdx++) {
            // For some reason, nn0 cannot see dn0~7 at all while nn1 cannot see dn4 and after
            if (nnIdx == 0 && dnIdx < 6 || nnIdx == 1 && dnIdx >= 4) {
              continue;
            }
            nns[nnIdx].sendHeartbeat(dns[dnIdx]);
            timer.advance(1);
          }
        }
        for (int nnIdx = 0; nnIdx < N_NNS; nnIdx++) {
          ((ThrottlerCalibrationMasterPolicyAverageActiveNodes) nns[nnIdx].policy).triggerCalibrationForTesting();
        }
        timer.advance(1000);
      }

      // dn 0~3 receives bandwidth suggestions from nn1 first, and first come first served
      for (int dnIdx = 0; dnIdx < 4; dnIdx++) {
        assertBandwidth(MASTER_BANDWIDTH / 4, dnIdx);
      }
      // dn 4~7 receives bandwidth suggestions from nn2 first, and first come first served
      for (int dnIdx = 4; dnIdx < 6; dnIdx++) {
        assertBandwidth(MASTER_BANDWIDTH / N_DNS, dnIdx);
      }
      // dn 8~ receives bandwidth suggestions from nn0 first, and first come first served
      for (int dnIdx = 6; dnIdx < N_DNS; dnIdx++) {
        assertBandwidth(MASTER_BANDWIDTH / (N_DNS - 6), dnIdx);
      }
    }

    {
      // This test needs at least 5 NNs
      assert N_NNS > 4;

      setupMocks(ThrottlerCalibrationMasterPolicyAverageActiveNodes.class,
          ThrottlerCalibrationSlavePolicyMedian.class, override, timer);

      for (int count = 0; count < 3; count++) {
        for (int dnIdx = 0; dnIdx < N_DNS; dnIdx++) {
          cachedXferServers.get(dns[dnIdx]).getReadThrottler().dummyThrottleForTesting(1000);
          cachedXferServers.get(dns[dnIdx]).getWriteThrottler().dummyThrottleForTesting(1000);
          cachedXferServers.get(dns[dnIdx]).getTransferThrottler().dummyThrottleForTesting(1000);
          for (int nnIdx = 0; nnIdx < N_NNS; nnIdx++) {
            // For some reason, nn0 cannot see dn0~7 at all while nn1 cannot see dn4 and after
            if (nnIdx == 0 && dnIdx < 6 || nnIdx == 1 && dnIdx >= 4) {
              continue;
            }
            nns[nnIdx].sendHeartbeat(dns[dnIdx]);
            timer.advance(1);
          }
        }
        for (int nnIdx = 0; nnIdx < N_NNS; nnIdx++) {
          ((ThrottlerCalibrationMasterPolicyAverageActiveNodes) nns[nnIdx].policy).triggerCalibrationForTesting();
        }
        timer.advance(1000);
      }

      // On median policy, all nodes should be the same
      for (int dnIdx = 0; dnIdx < N_DNS; dnIdx++) {
        assertBandwidth(MASTER_BANDWIDTH / N_DNS, dnIdx);
      }
    }
  }

  // Use as base for testing different policies
  @Test
  public void testRandomlyDroppedHeartbeats() {
    Configuration override = new Configuration();
    int nnCalibrationInterval = 4 * 3600;
    int dnCalibrationInterval = 3600;
    int testDuration = 24 * 3600;
    // NN calibrates every 4 hour, DN every hour
    override.setTimeDuration(
        DFSConfigKeys.DFS_THROTTLER_AVERAGE_ACTIVE_POLICY_CALIBRATION_INTERVAL_KEY,
        nnCalibrationInterval, TimeUnit.SECONDS);
    override.setTimeDuration(
        DFSConfigKeys.DFS_THROTTLER_FIRST_COME_FIRST_SERVED_POLICY_CALIBRATION_INTERVAL_KEY,
        dnCalibrationInterval, TimeUnit.SECONDS);
    override.setTimeDuration(DFSConfigKeys.DFS_THROTTLER_AVERAGE_ACTIVE_POLICY_GRACE_PERIOD_KEY, 30,
        TimeUnit.MINUTES);

    // Initialize with a big chunk of time to make sure NNs register DNs' initial heartbeats
    FakeTimer timer = new FakeTimer();
    timer.advance((long) 1E12);

    setupMocks(ThrottlerCalibrationMasterPolicyAverageActiveNodes.class,
        ThrottlerCalibrationSlavePolicyFirstComeFirstServed.class, override, timer);

    double heartbeatDropRate = 0.05;
    double responseDropRate = 0.05;

    // Heartbeat every 3s
    long step = 3;
    int lastCalibration = -1;
    for (int count = 0; count < testDuration; count += step) {
      timer.advance(step * 1000);
      for (int dnIdx = 0; dnIdx < N_DNS; dnIdx++) {
        cachedXferServers.get(dns[dnIdx]).getReadThrottler().dummyThrottleForTesting(1000);
        cachedXferServers.get(dns[dnIdx]).getWriteThrottler().dummyThrottleForTesting(1000);
        cachedXferServers.get(dns[dnIdx]).getTransferThrottler().dummyThrottleForTesting(1000);
        for (int nnIdx = 0; nnIdx < N_NNS; nnIdx++) {
          if (new Random().nextDouble() < heartbeatDropRate) {
            continue;
          }
          nns[nnIdx].sendHeartbeat(dns[dnIdx], responseDropRate);
        }
      }
      if (count / dnCalibrationInterval > lastCalibration) {
        lastCalibration = count / dnCalibrationInterval;
        long[] masterBandwidths = getMasterBandwidths();
        LOG.info("Bandwidth calibration {}: read={}, write={}, transfer={}", lastCalibration,
            masterBandwidths[0], masterBandwidths[1], masterBandwidths[2]);
      }
    }
  }

  @Test
  public void testNoCalibration() {
    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_DATANODE_DATA_READ_BANDWIDTHPERSEC_KEY, 9000);
    conf.setLong(DFSConfigKeys.DFS_DATANODE_DATA_WRITE_BANDWIDTHPERSEC_KEY, 9000);
    conf.setLong(DFSConfigKeys.DFS_DATANODE_DATA_TRANSFER_BANDWIDTHPERSEC_KEY, 9000);

    setupMocks(ThrottlerCalibrationMasterPolicyAverageActiveNodes.class,
        ThrottlerCalibrationSlavePolicyNoCalibration.class, conf, null);
    for (int count = 0; count < 2; count++) {
      for (int dnIdx = 0; dnIdx < N_DNS; dnIdx++) {
        cachedXferServers.get(dns[dnIdx]).getReadThrottler().dummyThrottleForTesting(100);
        cachedXferServers.get(dns[dnIdx]).getWriteThrottler().dummyThrottleForTesting(100);
        cachedXferServers.get(dns[dnIdx]).getTransferThrottler().dummyThrottleForTesting(100);
        for (int nnIdx = 0; nnIdx < N_NNS; nnIdx++) {
          nns[nnIdx].sendHeartbeat(dns[dnIdx]);
        }
      }
      for (int nnIdx = 0; nnIdx < N_NNS; nnIdx++) {
        ((ThrottlerCalibrationMasterPolicyAverageActiveNodes) nns[nnIdx].policy).triggerCalibrationForTesting();
      }
    }

    // Bandwidths do not change
    for (int dnIdx = 0; dnIdx < N_DNS; dnIdx++) {
      assertBandwidth(9000, dnIdx);
    }
  }

  private long[] getMasterBandwidths() {
    long[] masterBandwidths = new long[3];
    for (int dnIdx = 0; dnIdx < N_DNS; dnIdx++) {
      masterBandwidths[0] += cachedXferServers.get(dns[dnIdx]).getReadThrottler().getBandwidth();
      masterBandwidths[1] += cachedXferServers.get(dns[dnIdx]).getWriteThrottler().getBandwidth();
      masterBandwidths[2] +=
          cachedXferServers.get(dns[dnIdx]).getTransferThrottler().getBandwidth();
    }
    return masterBandwidths;
  }

  private void assertBandwidth(long testBandwidth, int dnIdx) {
    Assert.assertEquals(testBandwidth,
        cachedXferServers.get(dns[dnIdx]).getReadThrottler().getBandwidth());
    Assert.assertEquals(testBandwidth,
        cachedXferServers.get(dns[dnIdx]).getWriteThrottler().getBandwidth());
    Assert.assertEquals(testBandwidth,
        cachedXferServers.get(dns[dnIdx]).getTransferThrottler().getBandwidth());
  }
}
