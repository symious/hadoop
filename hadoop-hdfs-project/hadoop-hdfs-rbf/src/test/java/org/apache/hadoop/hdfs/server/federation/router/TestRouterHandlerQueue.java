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

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.fairness.RouterRpcFairnessPolicyController;
import org.apache.hadoop.hdfs.server.federation.fairness.StaticRouterRpcFairnessPolicyController;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.resolver.MultipleDestinationMountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.order.DestinationOrder;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider;
import org.apache.hadoop.io.retry.RetryInvocationHandler;
import org.apache.hadoop.ipc.DeepHandlerManager;
import org.apache.hadoop.ipc.ProcessingDetails;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.ipc.metrics.DeepRpcMetrics;
import org.apache.hadoop.ipc.metrics.RpcMetrics;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.test.MetricsAsserts.getDoubleGauge;
import static org.apache.hadoop.test.MetricsAsserts.getLongCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test the Router handlers fairness control rejects and accepts requests.
 */
public class TestRouterHandlerQueue {

  private static final Logger LOG = LoggerFactory.getLogger(TestRouterHandlerQueue.class);
  private static final long SLOW_RPC_TIME_MS = 2000;
  private static final int N_NS = 4;
  private static final int SURFACE_HANDLERS = 8;
  private static final int DEEP_HANDLERS = 15;
  private static final int MAX_DEEP_HANDLERS_PER_NAMESPACE = 10;
  private static final int DEEP_QUEUE_CAPACITY = 7;
  private static final int SLOW_RPC_CALLS = SURFACE_HANDLERS;
  private static final int FAST_RPC_CALLS = 5;
  private static final int TOTAL_RPC = SLOW_RPC_CALLS + FAST_RPC_CALLS;

  private StateStoreDFSCluster cluster;
  RouterContext routerContext;
  private AtomicInteger standbyExceptionsCaught;
  private RpcMetrics rpcMetrics;
  private DeepRpcMetrics deepRpcMetrics;
  private DeepHandlerManager deepHandlerManager;

  /**
   * Have to use a dummy spy class because of an issue in Mockito 1.8
   * that makes doAnswer().when() always synchronous, thus useless for
   * testing parallel calls.
   */
  class RouterRpcClientDummy extends RouterRpcClient {
    public RouterRpcClientDummy(Configuration conf, Router router, ActiveNamenodeResolver resolver,
        RouterRpcMonitor monitor) {
      super(conf, router, resolver, monitor);
    }

    @Override
    public Object invokeMethod(final UserGroupInformation ugi,
        final List<? extends FederationNamenodeContext> namenodes, final Class<?> protocol,
        final Method method, final Object... params) throws IOException {
      // Bias: calls not to ns0 always take at least SLOW_RPC_TIME_MS
      if (!namenodes.get(0).getNameserviceId().equals("ns0")) {
        try {
          Thread.sleep(SLOW_RPC_TIME_MS);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      return null;
    }
  }

  @BeforeClass
  public static void initialize() {
    LogManager.getLogger(ProcessingDetails.LOG.getName()).setLevel(Level.DEBUG);
    LogManager.getLogger(DeepHandlerManager.LOG.getName()).setLevel(Level.DEBUG);
    LogManager.getLogger(FSNamesystem.class.getName() + ".audit").setLevel(Level.WARN);
    LogManager.getLogger(RouterRpcServer.class.getName() + ".audit").setLevel(Level.WARN);
  }

  @After
  public void cleanup() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  private void setupCluster(boolean useDeepHandlers, int permitWaitTimeMs) throws Exception {
    // Build and start a federated cluster
    cluster = new StateStoreDFSCluster(false, N_NS, MultipleDestinationMountTableResolver.class);
    Configuration conf = new RouterConfigBuilder().stateStore().admin().rpc().build();
    conf.setBoolean(CommonConfigurationKeys.IGNORE_SDI_AUTHENTICATE_KEY, true);
    Configuration routerConf = new Configuration(conf);
    routerConf.setInt(RBFConfigKeys.DFS_ROUTER_WAIT_TIME_FOR_ACQUIRING_PERMIT_KEY,
        permitWaitTimeMs);
    routerConf.setClass(RBFConfigKeys.DFS_ROUTER_FAIRNESS_POLICY_CONTROLLER_CLASS,
        StaticRouterRpcFairnessPolicyController.class, RouterRpcFairnessPolicyController.class);
    routerConf.setInt(RBFConfigKeys.DFS_ROUTER_HANDLER_COUNT_KEY, SURFACE_HANDLERS);
    // Allow surface handlers to accept at most 1 call per namespace at a time for easier testing
    for (int i = 0; i < N_NS; i++) {
      routerConf.setInt(RBFConfigKeys.DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX + "ns" + i, 1);
    }
    if (useDeepHandlers) {
      routerConf.setBoolean(RBFConfigKeys.DFS_ROUTER_DEEP_HANDLER_ENABLED_KEY, true);
      routerConf.setInt(CommonConfigurationKeys.DFS_ROUTER_DEEP_HANDLER_COUNT_KEY, DEEP_HANDLERS);
      routerConf.setInt(CommonConfigurationKeys.DFS_ROUTER_DEEP_QUEUE_CAPACITY_KEY,
          DEEP_QUEUE_CAPACITY);
      routerConf.setDouble(
          CommonConfigurationKeys.DFS_ROUTER_DEEP_HANDLER_MAX_UTILIZATION_PERCENTAGE_KEY,
          (double) MAX_DEEP_HANDLERS_PER_NAMESPACE / DEEP_HANDLERS);
    }

    // Datanodes not needed for this test.
    cluster.setNumDatanodesPerNameservice(0);

    cluster.addRouterOverrides(routerConf);
    cluster.startCluster(conf);
    cluster.startRouters();
    cluster.registerNamenodes();
    cluster.waitNamenodeRegistration();
    routerContext = cluster.getRandomRouter();
    String[] nss = new String[N_NS];
    for (int i = 0; i < N_NS; i++) {
      createMountTableEntry("/test" + "ns" + i, new String[] { "ns" + i });
      nss[i] = "ns" + i;
    }
    createMountTableEntry("/test", nss);

    setupMocks();

    rpcMetrics = routerContext.getRouterRpcServer().getServer().getRpcMetrics();
    deepHandlerManager = routerContext.getRouterRpcServer().getServer().getDeepHandlerManager();
    deepRpcMetrics = deepHandlerManager == null ? null : deepHandlerManager.getMetrics();
  }

  private void setupMocks() {
    // Setup NS bias
    for (RouterContext router : cluster.getRouters()) {
      RouterRpcServer server = router.getRouterRpcServer();
      RouterRpcClientDummy dummyClient =
          new RouterRpcClientDummy(server.getConfig(), router.getRouter(),
              server.getNamenodeResolver(), server.getRPCMonitor());
      Whitebox.setInternalState(router.getRouterRpcServer(), "rpcClient", dummyClient);
      Whitebox.setInternalState(router.getRouterRpcServer().getClientProto(), "rpcClient",
          dummyClient);
    }
  }

  @Test
  public void testDeepHandlersEnabled() throws Exception {
    int permitWaitTimeMs = 300;
    setupCluster(true, permitWaitTimeMs);
    runTest(permitWaitTimeMs * 2, false);
    assertEquals(0, standbyExceptionsCaught.get());
    MetricsRecordBuilder builder = getMetrics(rpcMetrics.name());
    MetricsRecordBuilder deepBuilder = getMetrics(deepRpcMetrics.getName());
    double queueTimeAvg = getDoubleGauge("RpcQueueTimeAvgTime", builder);
    double queueTimeMax = getDoubleGauge("RpcQueueTimeIMaxTime", builder);
    double processingTimeAvg = getDoubleGauge("RpcProcessingTimeAvgTime", builder);
    double processingTimeMax = getDoubleGauge("RpcProcessingTimeIMaxTime", builder);

    // Because all ns0 calls are spawned a while after ns1 calls start,
    // only the first ns1 call stays in the original handler layer, the others already
    // went to the deep queue.
    // Thus, all ns0 calls should finish on the first layer as well without
    // going into the deep queue.

    // All ns0 calls have negligible processing time and queue time
    // All ns1 calls have SLOW_RPC_CALLS processing time, none throws overloaded exception
    // First ns1 call has short queue time, subsequent calls have permitWaitTimeMs queue time
    int deepCalls = SLOW_RPC_CALLS - 1;
    assertApproximate((long) queueTimeAvg, deepCalls * permitWaitTimeMs / TOTAL_RPC, (float) 0.1);
    assertApproximate((long) processingTimeAvg, SLOW_RPC_TIME_MS * SLOW_RPC_CALLS / TOTAL_RPC,
        (float) 0.1);
    assertApproximate((long) queueTimeMax, permitWaitTimeMs, (float) 0.1);
    assertApproximate((long) processingTimeMax, SLOW_RPC_TIME_MS, (float) 0.1);

    long ns1DeepCalls = getLongCounter("DeepCallAttempts_ns1", deepBuilder);
    assertEquals(deepCalls, ns1DeepCalls);
  }

  @Test
  public void testDeepHandlersEnabledAllNssQueued() throws Exception {
    int permitWaitTimeMs = 300;
    setupCluster(true, permitWaitTimeMs);
    runTest(permitWaitTimeMs * 2, true);
    assertEquals(0, standbyExceptionsCaught.get());
    MetricsRecordBuilder builder = getMetrics(rpcMetrics.name());
    MetricsRecordBuilder deepBuilder = getMetrics(deepRpcMetrics.getName());
    double queueTimeAvg = getDoubleGauge("RpcQueueTimeAvgTime", builder);
    double queueTimeMax = getDoubleGauge("RpcQueueTimeIMaxTime", builder);
    double processingTimeAvg = getDoubleGauge("RpcProcessingTimeAvgTime", builder);
    double processingTimeMax = getDoubleGauge("RpcProcessingTimeIMaxTime", builder);

    // Slow calls are spawned first, then all fast calls to ns0 are spawned
    // The wait between these 2 steps are long enough for the slow calls to be put into deep queues
    // Only the first slow calls to each namespace stay in the original handler layer
    // Thus, all ns0 calls should finish on the first layer as well without
    // going into the deep queue.

    // All fast calls have negligible processing time and queue time
    // All slow calls have SLOW_RPC_CALLS processing time, none throws overloaded exception
    // First ns1/ns2 calls have short queue time, subsequent calls have permitWaitTimeMs queue time
    int deepCalls = SLOW_RPC_CALLS - (N_NS - 1);
    assertApproximate((long) queueTimeAvg, deepCalls * permitWaitTimeMs / TOTAL_RPC, (float) 0.1);
    assertApproximate((long) processingTimeAvg, SLOW_RPC_TIME_MS * SLOW_RPC_CALLS / TOTAL_RPC,
        (float) 0.1);
    assertApproximate((long) queueTimeMax, permitWaitTimeMs, (float) 0.1);
    assertApproximate((long) processingTimeMax, SLOW_RPC_TIME_MS, (float) 0.1);

    for (int i = 1; i < N_NS; i++) {
      int nCall = SLOW_RPC_CALLS / (N_NS - 1);
      if (i == N_NS - 1) {
        nCall = SLOW_RPC_CALLS - nCall * (N_NS - 2);
      }
      long realNsDeepCalls = getLongCounter("DeepCallAttempts_ns" + i, deepBuilder);
      // nCall - 1 because one call finishes in the surface layer
      assertEquals(nCall - 1, realNsDeepCalls);
    }
  }

  @Test
  public void testDefaultHandlersWithTimeout() throws Exception {
    int permitWaitTimeMs = 1000;
    setupCluster(false, permitWaitTimeMs);
    runTest(50, false);
    assertEquals(SLOW_RPC_CALLS - 1, standbyExceptionsCaught.get());
    // All ns0 calls have short processing time but 1s queue time
    // 2 out of 3 ns1 calls have short queue time but 1s processing time then timeout
    // The other ns1 call has short queue time and 2s processing time
    MetricsRecordBuilder builder = getMetrics(rpcMetrics.name());
    double queueTimeAvg = getDoubleGauge("RpcQueueTimeAvgTime", builder);
    double queueTimeMax = getDoubleGauge("RpcQueueTimeIMaxTime", builder);
    double processingTimeAvg = getDoubleGauge("RpcProcessingTimeAvgTime", builder);
    double processingTimeMax = getDoubleGauge("RpcProcessingTimeIMaxTime", builder);

    assertApproximate((long) queueTimeAvg, FAST_RPC_CALLS * permitWaitTimeMs / TOTAL_RPC,
        (float) 0.05);
    assertApproximate((long) processingTimeAvg,
        (SLOW_RPC_TIME_MS + (SLOW_RPC_CALLS - 1) * permitWaitTimeMs) / TOTAL_RPC, (float) 0.05);
    assertApproximate((long) queueTimeMax, permitWaitTimeMs, (float) 0.05);
    assertApproximate((long) processingTimeMax, SLOW_RPC_TIME_MS, (float) 0.05);
  }

  @Test(timeout = 40000)
  public void testDefaultHandlersWithoutTimeout() throws Exception {
    setupCluster(false, 100000000);
    runTest(50, false);
    // Should get no overloaded exceptions
    assertEquals(0, standbyExceptionsCaught.get());

    // Everything should finish without throwing any overloaded exception
    // They just take very long to finish
    MetricsRecordBuilder builder = getMetrics(rpcMetrics.name());
    double queueTimeAvg = getDoubleGauge("RpcQueueTimeAvgTime", builder);
    double queueTimeMax = getDoubleGauge("RpcQueueTimeIMaxTime", builder);
    double processingTimeAvg = getDoubleGauge("RpcProcessingTimeAvgTime", builder);
    double processingTimeMax = getDoubleGauge("RpcProcessingTimeIMaxTime", builder);

    // First ns1 call has negligible queue time, SLOW_RPC_TIME_MS processing time
    // All ns0 calls have SLOW_RPC_TIME_MS queue time and negligible processing time
    // 2nd ns1 call takes 2*SLOW_RPC_TIME_MS processing time
    // 3rd ns1 call takes 3*SLOW_RPC_TIME_MS processing time
    assertApproximate((long) queueTimeAvg, FAST_RPC_CALLS * SLOW_RPC_TIME_MS / TOTAL_RPC,
        (float) 0.05);
    assertApproximate((long) processingTimeAvg,
        SLOW_RPC_CALLS * (SLOW_RPC_CALLS + 1) / 2 * SLOW_RPC_TIME_MS / TOTAL_RPC, (float) 0.05);
    assertApproximate((long) queueTimeMax, SLOW_RPC_TIME_MS, (float) 0.05);
    assertApproximate((long) processingTimeMax, SLOW_RPC_TIME_MS * SLOW_RPC_CALLS, (float) 0.05);
  }

  @Test
  public void testDeepHandlersQueueOverflow() throws Exception {
    int permitWaitTimeMs = 30;
    standbyExceptionsCaught = new AtomicInteger(0);
    setupCluster(true, permitWaitTimeMs);

    List<Thread> slowThreads = new ArrayList<>();

    DFSClient routerClient =
        new DFSClient(routerContext.getFileSystemURI(), new HdfsConfiguration());
    // 1 executed by a dedicated surface handler
    // All calls in the queue can stay in the queue
    // All calls in can be handled by MAX_DEEP_HANDLERS_PER_NAMESPACE are handled by deep handlers
    // 1 more call is held and blocked by DeepQueueWatcher
    // Hence the "1 + DEEP_QUEUE_CAPACITY + MAX_DEEP_HANDLERS_PER_NAMESPACE + 1" part
    // Any calls after this are discarded and thrown back to client
    int expectedFailedCalls = 5;
    int totalCalls =
        1 + DEEP_QUEUE_CAPACITY + MAX_DEEP_HANDLERS_PER_NAMESPACE + 1 + expectedFailedCalls;

    queueThread("ns1", routerClient, totalCalls, slowThreads);

    for (Thread thread : slowThreads) {
      thread.start();
    }

    for (Thread thread : slowThreads) {
      thread.join();
    }

    assertEquals(expectedFailedCalls, standbyExceptionsCaught.get());

    MetricsRecordBuilder deepBuilder = getMetrics(deepRpcMetrics.getName());
    long realDeepCalls = getLongCounter("DeepCallAttempts_ns1", deepBuilder);
    assertEquals(totalCalls - 1, realDeepCalls);
  }

  @Test
  public void testRPCOverloadClientFailover() throws Exception {
    testRPCOverloadClientFailoverInternal(false);
    testRPCOverloadClientFailoverInternal(true);
  }

  private void testRPCOverloadClientFailoverInternal(boolean useDeepHandlers) throws Exception {
    try {
      // See testDeepHandlersQueueOverflow for details on the maths
      int slowCalls =
          useDeepHandlers ? 1 + DEEP_QUEUE_CAPACITY + MAX_DEEP_HANDLERS_PER_NAMESPACE + 1 : 1;

      setupCluster(useDeepHandlers, 1);
      List<Thread> slowThreads = new ArrayList<>();
      DFSClient routerClient =
          new DFSClient(routerContext.getFileSystemURI(), new HdfsConfiguration());
      queueThread("ns1", routerClient, slowCalls, slowThreads);
      for (Thread thread : slowThreads) {
        thread.start();
      }
      // Small sleep to make sure the slow threads spawn first
      Thread.sleep(40);

      Configuration clientConf = new Configuration(cluster.getRouterClientConf());
      clientConf.set(HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX + "." + "fed",
          ConfiguredFailoverProxyProvider.class.getName());
      final String namenode = "r0";
      clientConf.set(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + ".fed", namenode);
      clientConf.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + ".fed." + namenode,
          routerContext.getFileSystemURI().toString());
      routerClient = new DFSClient(URI.create("hdfs://fed"), clientConf);

      // Has to failover then succeeds instead of throwing
      GenericTestUtils.LogCapturer logs =
          GenericTestUtils.LogCapturer.captureLogs(RetryInvocationHandler.LOG);
      routerClient.getFileInfo("/testns1/test.txt");
      assertTrue(logs.getOutput().contains("Trying to failover"));

      for (Thread thread : slowThreads) {
        thread.join();
      }

    } finally {
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * Executes test then parses log for rpc details
   */
  private void runTest(long delay, boolean queueAllNss) throws IOException, InterruptedException {
    standbyExceptionsCaught = new AtomicInteger(0);

    List<Thread> slowThreads = new ArrayList<>();
    List<Thread> fastThreads = new ArrayList<>();

    DFSClient routerClient =
        new DFSClient(routerContext.getFileSystemURI(), new HdfsConfiguration());
    for (int i = 0; i < N_NS; i++) {
      if (!queueAllNss && i > 1) {
        break;
      }
      if (i == 0) {
        queueThread("ns" + i, routerClient, FAST_RPC_CALLS, fastThreads);
      } else {
        int nCall = SLOW_RPC_CALLS / (N_NS - 1);
        if (!queueAllNss) {
          nCall = SLOW_RPC_CALLS;
        } else if (i == N_NS - 1) {
          nCall = SLOW_RPC_CALLS - nCall * (N_NS - 2);
        }
        queueThread("ns" + i, routerClient, nCall, slowThreads);
      }
    }

    // Start 3 long blocking ns1 threads that hog up all handlers
    for (Thread thread : slowThreads) {
      thread.start();
    }
    // Sleep a bit to make sure all ns1 call threads are up and running
    Thread.sleep(delay);

    // Start all normal threads for calls to ns0.
    for (Thread thread : fastThreads) {
      thread.start();
    }

    for (Thread thread : slowThreads) {
      thread.join();
    }
    for (Thread thread : fastThreads) {
      thread.join();
    }
  }

  private void queueThread(final String dest, final DFSClient routerClient, int numOps,
      List<Thread> threads) {
    for (int i = 0; i < numOps; i++) {
      Thread thread = new Thread(new Runnable() {
        @Override
        public void run() {
          ClientProtocol routerProto = routerClient.getNamenode();
          try {
            if (dest == null) {
              routerProto.getFileInfo("/test/test.txt");
            } else {
              if (dest.equals("ns0")) {
                routerProto.getFileInfo("/test" + dest + "/test.txt");
              } else {
                // Call getContentSummary to distinguish with fast ns0 getFileInfo calls
                // Also will throw NPE to make testing easier
                routerProto.getContentSummary("/test" + dest + "/test.txt");
              }
            }
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      });
      Thread.UncaughtExceptionHandler h = new Thread.UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread th, Throwable ex) {
          if (ex instanceof RuntimeException && ex.getCause() instanceof RemoteException
              && ((RemoteException) ex.getCause()).getClassName()
              .equals(StandbyException.class.getCanonicalName())) {
            standbyExceptionsCaught.incrementAndGet();
          }
        }
      };
      thread.setUncaughtExceptionHandler(h);
      threads.add(thread);
    }
  }

  private void createMountTableEntry(final String mountPoint, final String[] targets)
      throws Exception {

    RouterClient admin = routerContext.getAdminClient();
    MountTableManager mountTable = admin.getMountTableManager();
    Map<String, String> destMap = new HashMap<>();
    for (String target : targets) {
      destMap.put(target, mountPoint);
    }
    MountTable newEntry = MountTable.newInstance(mountPoint, destMap);
    newEntry.setDestOrder(DestinationOrder.HASH);
    AddMountTableEntryRequest addRequest = AddMountTableEntryRequest.newInstance(newEntry);
    AddMountTableEntryResponse addResponse = mountTable.addMountTableEntry(addRequest);
    boolean created = addResponse.getStatus();
    assertTrue(created);

    // Refresh the caches to get the mount table
    Router router = routerContext.getRouter();
    StateStoreService stateStore = router.getStateStore();
    stateStore.refreshCaches(true);
  }

  private void assertApproximate(long tester, long target, float epsilon) {
    float ratio = (float) tester / target;
    assertTrue(
        String.format("Value %s is outside expected range: target=%s, epsilon=%s", tester, target,
            epsilon), 1 - epsilon < ratio && ratio < 1 + epsilon);
  }
}
