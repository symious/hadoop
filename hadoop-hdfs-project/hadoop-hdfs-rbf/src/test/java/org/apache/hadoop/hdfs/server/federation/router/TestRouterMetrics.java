/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.metrics.FederationRPCMetrics;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import java.lang.management.ManagementFactory;
import java.util.List;

public class TestRouterMetrics {

  private StateStoreDFSCluster cluster;

  @After
  public void cleanup() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  private void setupCluster()
      throws Exception {
    // Build and start a federated cluster
    cluster = new StateStoreDFSCluster(false, 3);
    Configuration routerConf = new RouterConfigBuilder()
        .stateStore()
        .metrics()
        .admin()
        .rpc()
        .heartbeat()
        .build();

    cluster.setNumDatanodesPerNameservice(2);

    cluster.addRouterOverrides(routerConf);
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp();
  }

  @Test
  public void testMetricsFormatting() throws Exception {
    setupCluster();
    List<RouterContext> routers = cluster.getRouters();

    RouterRpcClient client0 = routers.get(0).getRouter().getRpcServer().getRPCClient();
    FederationRPCMetrics rpcMetrics0 = routers.get(0).getRouter().getRpcServer().getRPCMetrics();

    // Add 10 rejects from ns1, 5 from ns2 to ns0
    for (int i = 0; i < 10; i++) {
      client0.incrRejectedPermitForNs(routers.get(1).getNameserviceId());
    }
    for (int i = 0; i < 5; i++) {
      client0.incrRejectedPermitForNs(routers.get(2).getNameserviceId());
    }

    // Add 20 accepts from ns1, 15 from ns2 to ns0
    for (int i = 0; i < 20; i++) {
      client0.incrAcceptedPermitForNs(routers.get(1).getNameserviceId());
    }
    for (int i = 0; i < 15; i++) {
      client0.incrAcceptedPermitForNs(routers.get(2).getNameserviceId());
    }

    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    CompositeData rejectedReport = (CompositeData) mBeanServer
        .getAttribute(new ObjectName("Hadoop:service=Router,name=FederationRPC"),
            "ProxyOpPermitRejectedPerNsJSON");
    CompositeData rejectedMetrics = rpcMetrics0.getProxyOpPermitRejectedPerNsJSON();

    CompositeData acceptedReport = (CompositeData) mBeanServer
        .getAttribute(new ObjectName("Hadoop:service=Router,name=FederationRPC"),
            "ProxyOpPermitAcceptedPerNsJSON");
    CompositeData acceptedMetrics = rpcMetrics0.getProxyOpPermitAcceptedPerNsJSON();

    Assert.assertEquals(rejectedMetrics, rejectedReport);
    Assert.assertEquals(acceptedMetrics, acceptedReport);
    Assert.assertEquals((long) rejectedMetrics.get(routers.get(1).getNameserviceId()), 10);
    Assert.assertEquals((long) rejectedMetrics.get(routers.get(2).getNameserviceId()), 5);
    Assert.assertEquals((long) acceptedMetrics.get(routers.get(1).getNameserviceId()), 20);
    Assert.assertEquals((long) acceptedMetrics.get(routers.get(2).getNameserviceId()), 15);
  }
}