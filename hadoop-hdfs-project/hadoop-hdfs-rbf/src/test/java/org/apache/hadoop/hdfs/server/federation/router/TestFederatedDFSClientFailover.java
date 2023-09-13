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
package org.apache.hadoop.hdfs.server.federation.router;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.MultipleDestinationMountTableResolver;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider;
import org.apache.hadoop.io.retry.RetryInvocationHandler;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.Whitebox;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

public class TestFederatedDFSClientFailover {
  private static final Logger LOG = LoggerFactory.getLogger(TestFederatedDFSClientFailover.class);
  private static StateStoreDFSCluster cluster;
  private MiniRouterDFSCluster.RouterContext routerContext;

  @Before
  public void setUpCluster() throws Exception {
    cluster = new StateStoreDFSCluster(false, 2, MultipleDestinationMountTableResolver.class);
    Configuration conf = new RouterConfigBuilder().stateStore().admin().rpc().build();
    conf.setBoolean(CommonConfigurationKeys.IGNORE_SDI_AUTHENTICATE_KEY, true);
    Configuration routerConf = new Configuration(conf);
    cluster.setNumDatanodesPerNameservice(0);

    cluster.addRouterOverrides(routerConf);
    cluster.startCluster(conf);
    cluster.startRouters();
    cluster.registerNamenodes();
    cluster.waitNamenodeRegistration();
    routerContext = cluster.getRandomRouter();
  }

  @After
  public void tearDownCluster() {
    if (cluster != null) {
      cluster.stopRouter(routerContext);
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testClientFailoverOnFailedNNResolution() throws IOException, InterruptedException {
    // Make ns0 unresolvable from routers
    List<ActiveNamenodeResolver> originalResolvers = new ArrayList<>();
    for (MiniRouterDFSCluster.RouterContext router : cluster.getRouters()) {
      originalResolvers.add(router.getRouterRpcClient().getNamenodeResolver());
      ActiveNamenodeResolver spyNnResolver = spy(router.getRouterRpcClient().getNamenodeResolver());
      doAnswer(invocationOnMock -> {
        String ns = invocationOnMock.getArgument(0);
        if (ns.equals("ns0")) {
          return new ArrayList<>();
        } else {
          return invocationOnMock.callRealMethod();
        }
      }).when(spyNnResolver).getNamenodesForNameserviceId(anyString(), anyBoolean());
      Whitebox.setInternalState(router.getRouterRpcClient(), "namenodeResolver", spyNnResolver);
    }

    // Client with failover configured
    Configuration clientConf = new Configuration(cluster.getRouterClientConf());
    clientConf.set(HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX + "." + "fed",
        ConfiguredFailoverProxyProvider.class.getName());
    final String namenode = "r0";
    clientConf.set(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + ".fed", namenode);
    clientConf.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + ".fed." + namenode,
        routerContext.getFileSystemURI().toString());
    DFSClient routerClient = new DFSClient(URI.create("hdfs://fed"), clientConf);
    GenericTestUtils.LogCapturer logs =
        GenericTestUtils.LogCapturer.captureLogs(RetryInvocationHandler.LOG);

    // Make an RPC call in a thread so that it doesn't block the unit test
    Thread thread = new Thread(() -> {
      try {
        routerClient.getFileInfo("/test.txt");
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    thread.start();

    // Sleep to make the RPC call fail
    Thread.sleep(500);
    // Restore NN resolvers to routers, RPC call should pass after this
    int i = 0;
    for (MiniRouterDFSCluster.RouterContext router : cluster.getRouters()) {
      Whitebox.setInternalState(router.getRouterRpcClient(), "namenodeResolver",
          originalResolvers.get(i));
      i++;
    }
    thread.join();
    assertTrue(logs.getOutput().contains("Trying to failover"));
  }
}
