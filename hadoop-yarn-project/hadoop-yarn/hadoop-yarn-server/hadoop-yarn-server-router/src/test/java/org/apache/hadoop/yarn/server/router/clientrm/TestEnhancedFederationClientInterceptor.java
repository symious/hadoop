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

package org.apache.hadoop.yarn.server.router.clientrm;

import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.manager.UniformBroadcastPolicyManager;
import org.apache.hadoop.yarn.server.federation.store.impl.MemoryFederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreTestUtil;
import org.apache.hadoop.yarn.server.router.utils.RouterRpcRequestCache;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

/**
 * Extends the {@code BaseRouterClientRMTest} and overrides methods in order to
 * use the {@code RouterClientRMService} pipeline test cases for testing the
 * {@code FederationInterceptor} class. The tests for
 * {@code RouterClientRMService} has been written cleverly so that it can be
 * reused to validate different request intercepter chains.
 */
public class TestEnhancedFederationClientInterceptor extends BaseRouterClientRMTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestEnhancedFederationClientInterceptor.class);

  private TestableEnhancedFederationClientInterceptor interceptor;
  private MemoryFederationStateStore stateStore;
  private FederationStateStoreTestUtil stateStoreUtil;
  private List<SubClusterId> subClusters;

  private String user = "test-user";

  private final static int NUM_SUBCLUSTER = 4;

  private final static int TEST_RPC_CACHE_TIME = 30;

  @Override
  public void setUp() {
    super.setUpConfig();
    interceptor = new TestableEnhancedFederationClientInterceptor();

    stateStore = new MemoryFederationStateStore();
    stateStore.init(this.getConf());
    FederationStateStoreFacade.getInstance().reinitialize(stateStore,
        getConf());
    stateStoreUtil = new FederationStateStoreTestUtil(stateStore);

    RouterRpcRequestCache.getInstance().reinitialize(getConf());

    interceptor.setConf(this.getConf());
    interceptor.init(user);

    subClusters = new ArrayList<SubClusterId>();

    try {
      for (int i = 0; i < NUM_SUBCLUSTER; i++) {
        SubClusterId sc = SubClusterId.newInstance(Integer.toString(i));
        stateStoreUtil.registerSubCluster(sc);
        subClusters.add(sc);
      }
    } catch (YarnException e) {
      LOG.error(e.getMessage());
      Assert.fail();
    }

  }

  @Override
  public void tearDown() {
    interceptor.shutdown();
    super.tearDown();
  }

  @Override
  protected YarnConfiguration createConfiguration() {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.FEDERATION_ENABLED, true);
    String mockPassThroughInterceptorClass =
        PassThroughClientRequestInterceptor.class.getName();

    // Create a request intercepter pipeline for testing. The last one in the
    // chain is the federation intercepter that calls the mock resource manager.
    // The others in the chain will simply forward it to the next one in the
    // chain
    conf.set(YarnConfiguration.ROUTER_CLIENTRM_INTERCEPTOR_CLASS_PIPELINE,
        mockPassThroughInterceptorClass + "," + mockPassThroughInterceptorClass
            + "," + TestableFederationClientInterceptor.class.getName());

    conf.set(YarnConfiguration.FEDERATION_POLICY_MANAGER,
        UniformBroadcastPolicyManager.class.getName());

    // Disable StateStoreFacade cache
    conf.setInt(YarnConfiguration.FEDERATION_CACHE_TIME_TO_LIVE_SECS, 0);

    conf.setInt(YarnConfiguration.ROUTER_RPC_CACHE_TIME_TO_LIVE_SECS,
        TEST_RPC_CACHE_TIME);

    return conf;
  }

  @Test
  public void testGetClusterNodesRequestWithCache()
      throws YarnException, IOException, InterruptedException {
    LOG.info(
        "Test FederationClientInterceptor : Get Cluster Nodes request with cache");

    GetClusterNodesRequest getClusterNodesRequest = GetClusterNodesRequest.newInstance(
        EnumSet.of(NodeState.RUNNING));

    // First request and create cache
    long startTime_1 = Time.monotonicNow();
    GetClusterNodesResponse response =
        interceptor.getClusterNodes(getClusterNodesRequest);
    Assert.assertEquals(subClusters.size(), response.getNodeReports().size());
    long endTime_1 = Time.monotonicNow();
    long request1_costTime = endTime_1 - startTime_1;
    LOG.info("request1_costTime = " + request1_costTime);

    // Second request directly get from cache
    Thread.sleep(1 * 1000);
    long startTime_2 = Time.monotonicNow();
    response =
        interceptor.getClusterNodes(getClusterNodesRequest);
    Assert.assertEquals(subClusters.size(), response.getNodeReports().size());
    long endTime_2 = Time.monotonicNow();
    long request2_costTime = endTime_2 - startTime_2;
    LOG.info("request2_costTime = " + request2_costTime);

    // Third request directly get from cache too, because cache is not timeout
    Thread.sleep(10 * 1000);
    long startTime_3 = Time.monotonicNow();
    response = interceptor.getClusterNodes(getClusterNodesRequest);
    Assert.assertEquals(subClusters.size(), response.getNodeReports().size());
    long endTime_3 = Time.monotonicNow();
    long request3_costTime = endTime_3 - startTime_3;
    LOG.info("request3_costTime = " + request3_costTime);

    // Fourth request cache timeout and reload again
    Thread.sleep(TEST_RPC_CACHE_TIME * 1000);
    long startTime_4 = Time.monotonicNow();
    response = interceptor.getClusterNodes(getClusterNodesRequest);
    Assert.assertEquals(subClusters.size(), response.getNodeReports().size());
    long endTime_4 = Time.monotonicNow();
    long request4_costTime = endTime_4 - startTime_4;
    LOG.info("request4_costTime = " + request4_costTime);

    Assert.assertTrue(request1_costTime > request2_costTime);
    Assert.assertTrue(request4_costTime > request2_costTime);

    Assert.assertTrue(request1_costTime > request3_costTime);
    Assert.assertTrue(request4_costTime > request3_costTime);

  }
}
