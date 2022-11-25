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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.HdfsConfiguration;
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
import org.apache.hadoop.ipc.DeepHandlerManager;
import org.apache.hadoop.ipc.OverloadedNameserviceException;
import org.apache.hadoop.ipc.ProcessingDetails;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.metrics.RpcMetrics;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.test.MetricsAsserts.getDoubleGauge;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test the Router handlers fairness control rejects and accepts requests.
 */
public class TestRouterHandlerQueue {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRouterHandlerQueue.class);
  private static final long LONG_RPC_TIME_MS = 2000;
  private static final int SURFACE_HANDLERS = 3;
  private static final int DEEP_HANDLERS = 10;
  private static final int LONG_HANDLER_LIMIT = 5;
  private static final int SHORT_HANDLER_LIMIT = 5;
  private static final int DEEP_QUEUE_CAPACITY = 7;
  private static final int LONG_RPC_CALLS = SURFACE_HANDLERS;
  private static final int SHORT_RPC_CALLS = 5;
  private static final int TOTAL_RPC = LONG_RPC_CALLS + SHORT_RPC_CALLS;

  private StateStoreDFSCluster cluster;
  RouterContext routerContext;
  private AtomicInteger overloadedExceptionCaught;
  private RpcMetrics rpcMetrics;

  /**
   * Have to use a dummy spy class because of an issue in Mockito 1.8
   * that makes doAnswer().when() always synchronous, thus useless for
   * testing parallel calls.
   */
  class RouterRpcClientDummy extends RouterRpcClient {
    public RouterRpcClientDummy(Configuration conf, Router router,
        ActiveNamenodeResolver resolver, RouterRpcMonitor monitor) {
      super(conf, router, resolver, monitor);
    }

    @Override
    public Object invokeMethod(final UserGroupInformation ugi,
        final List<? extends FederationNamenodeContext> namenodes,
        final Class<?> protocol, final Method method, final Object... params)
        throws IOException {
      // Bias calls to ns1 always take at least 2 seconds
      if (namenodes.get(0).getNameserviceId().equals("ns1")) {
        try {
          Thread.sleep(LONG_RPC_TIME_MS);
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
    LogManager.getLogger(FSNamesystem.class.getName() + ".audit")
        .setLevel(Level.WARN);
    LogManager.getLogger(RouterRpcServer.class.getName() + ".audit")
        .setLevel(Level.WARN);
  }

  @After
  public void cleanup() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  private void setupCluster(boolean useDeepHandlers, int permitWaitTimeMs)
      throws Exception {
    // Build and start a federated cluster
    cluster = new StateStoreDFSCluster(false, 2,
        MultipleDestinationMountTableResolver.class);
    Configuration conf =
        new RouterConfigBuilder().stateStore().admin().rpc().build();
    conf.setBoolean(CommonConfigurationKeys.IGNORE_SDI_AUTHENTICATE_KEY, true);
    Configuration routerConf = new Configuration(conf);
    routerConf.setInt(
        RBFConfigKeys.DFS_ROUTER_WAIT_TIME_FOR_ACQUIRING_PERMIT_KEY,
        permitWaitTimeMs);
    routerConf.setClass(
        RBFConfigKeys.DFS_ROUTER_FAIRNESS_POLICY_CONTROLLER_CLASS,
        StaticRouterRpcFairnessPolicyController.class,
        RouterRpcFairnessPolicyController.class);
    routerConf.setInt(RBFConfigKeys.DFS_ROUTER_HANDLER_COUNT_KEY,
        SURFACE_HANDLERS);
    if (useDeepHandlers) {
      routerConf.setBoolean(RBFConfigKeys.DFS_ROUTER_DEEP_HANDLER_ENABLED_KEY,
          true);
      routerConf.setInt(CommonConfigurationKeys.DFS_ROUTER_DEEP_HANDLER_COUNT_KEY,
          DEEP_HANDLERS);
      routerConf.setInt(CommonConfigurationKeys.DFS_ROUTER_DEEP_QUEUE_CAPACITY_KEY,
          DEEP_QUEUE_CAPACITY);
      routerConf.setDouble(
          CommonConfigurationKeys.DFS_ROUTER_DEEP_HANDLER_MAX_UTILIZATION_PERCENTAGE_KEY,
          (double) SHORT_HANDLER_LIMIT / DEEP_HANDLERS);
    }

    // Datanodes not needed for this test.
    cluster.setNumDatanodesPerNameservice(0);

    cluster.addRouterOverrides(routerConf);
    cluster.startCluster(conf);
    cluster.startRouters();
    cluster.registerNamenodes();
    cluster.waitNamenodeRegistration();
    routerContext = cluster.getRandomRouter();
    createMountTableEntry("/test", new String[] { "ns0", "ns1" });
    createMountTableEntry("/testns0", new String[] { "ns0" });
    createMountTableEntry("/testns1", new String[] { "ns1" });

    setupMocks();

    rpcMetrics = routerContext.getRouterRpcServer().getServer().getRpcMetrics();
  }

  private void setupMocks() {
    // Setup NS bias
    for (RouterContext router : cluster.getRouters()) {
      RouterRpcServer server = router.getRouterRpcServer();
      RouterRpcClientDummy dummyClient =
          new RouterRpcClientDummy(server.getConfig(), router.getRouter(),
              server.getNamenodeResolver(), server.getRPCMonitor());
      Whitebox.setInternalState(router.getRouterRpcServer(), "rpcClient",
          dummyClient);
      Whitebox.setInternalState(router.getRouterRpcServer().getClientProto(),
          "rpcClient", dummyClient);
    }
  }

  @Test
  public void testDeepHandlersEnabledAllCallsToDeepQueue() throws Exception {
    int permitWaitTimeMs = 300;
    setupCluster(true, permitWaitTimeMs);
    runTest(50);
    assertEquals(0, overloadedExceptionCaught.get());
    MetricsRecordBuilder builder = getMetrics(rpcMetrics.name());
    double queueTimeAvg = getDoubleGauge("RpcQueueTimeAvgTime", builder);
    double queueTimeMax = getDoubleGauge("RpcQueueTimeIMaxTime", builder);
    double processingTimeAvg = getDoubleGauge("RpcProcessingTimeAvgTime", builder);
    double processingTimeMax = getDoubleGauge("RpcProcessingTimeIMaxTime", builder);

    // Because all ns0 calls are spawned shortly after ns1 calls start,
    // all ns1 calls are still occupying the handlers.
    // Thus, all ns0 calls should timeout after permitWaitTimeMs
    // then go to the deep queue.

    // All ns0 calls have permitWaitTimeMs queue time, very short processing time
    // All ns1 calls have LONG_RPC_CALLS processing time, none throws overloaded exception
    // First ns1 call has short queue time, subsequent calls have permitWaitTimeMs queue time
    assertApproximate((long) queueTimeAvg,
        (SHORT_RPC_CALLS + LONG_RPC_CALLS - 1) * permitWaitTimeMs / TOTAL_RPC,
        (float) 0.15);
    assertApproximate((long) processingTimeAvg,
        LONG_RPC_TIME_MS * LONG_RPC_CALLS / TOTAL_RPC, (float) 0.05);
    assertApproximate((long) queueTimeMax, permitWaitTimeMs, (float) 0.05);
    assertApproximate((long) processingTimeMax, LONG_RPC_TIME_MS, (float) 0.05);
  }

  @Test
  public void testDeepHandlersEnabledOnlyNs1ToDeepQueue() throws Exception {
    int permitWaitTimeMs = 300;
    setupCluster(true, permitWaitTimeMs);
    runTest(1000);
    assertEquals(0, overloadedExceptionCaught.get());
    MetricsRecordBuilder builder = getMetrics(rpcMetrics.name());
    double queueTimeAvg = getDoubleGauge("RpcQueueTimeAvgTime", builder);
    double queueTimeMax = getDoubleGauge("RpcQueueTimeIMaxTime", builder);
    double processingTimeAvg = getDoubleGauge("RpcProcessingTimeAvgTime", builder);
    double processingTimeMax = getDoubleGauge("RpcProcessingTimeIMaxTime", builder);

    // Because all ns0 calls are spawned a while after ns1 calls start,
    // only the first ns1 stays in the original handler layer, the others already
    // went to the deep queue.
    // Thus, all ns0 calls should finish on the first layer as well without
    // going into the deep queue.

    // All ns0 calls have negligible processing time and queue time
    // All ns1 calls have LONG_RPC_CALLS processing time, none throws overloaded exception
    // First ns1 call has short queue time, subsequent calls have permitWaitTimeMs queue time
    assertApproximate((long) queueTimeAvg,
        (LONG_RPC_CALLS - 1) * permitWaitTimeMs / TOTAL_RPC,
        (float) 0.1);
    assertApproximate((long) processingTimeAvg,
        LONG_RPC_TIME_MS * LONG_RPC_CALLS / TOTAL_RPC, (float) 0.05);
    assertApproximate((long) queueTimeMax, permitWaitTimeMs, (float) 0.05);
    assertApproximate((long) processingTimeMax, LONG_RPC_TIME_MS, (float) 0.05);
  }

  @Test
  public void testDefaultHandlersWithTimeout() throws Exception {
    int permitWaitTimeMs = 1000;
    setupCluster(false, permitWaitTimeMs);
    runTest(50);
    // Should get 2 overloaded exceptions
    assertEquals(2, overloadedExceptionCaught.get());
    // All ns0 calls have short processing time but 1s queue time
    // 2 out of 3 ns1 calls have short queue time but 1s processing time then timeout
    // The other ns1 call has short queue time and 2s processing time
    MetricsRecordBuilder builder = getMetrics(rpcMetrics.name());
    double queueTimeAvg = getDoubleGauge("RpcQueueTimeAvgTime", builder);
    double queueTimeMax = getDoubleGauge("RpcQueueTimeIMaxTime", builder);
    double processingTimeAvg = getDoubleGauge("RpcProcessingTimeAvgTime", builder);
    double processingTimeMax = getDoubleGauge("RpcProcessingTimeIMaxTime", builder);

    assertApproximate((long) queueTimeAvg,
        SHORT_RPC_CALLS * permitWaitTimeMs / TOTAL_RPC, (float) 0.05);
    assertApproximate((long) processingTimeAvg,
        (LONG_RPC_TIME_MS + (LONG_RPC_CALLS - 1) * permitWaitTimeMs)
            / TOTAL_RPC, (float) 0.05);
    assertApproximate((long) queueTimeMax, permitWaitTimeMs, (float) 0.05);
    assertApproximate((long) processingTimeMax, LONG_RPC_TIME_MS, (float) 0.05);
  }

  @Test(timeout = 40000)
  public void testDefaultHandlersWithoutTimeout() throws Exception {
    setupCluster(false, 100000000);
    runTest(50);
    // Should get no overloaded exceptions
    assertEquals(0, overloadedExceptionCaught.get());

    // Everything should finish without throwing any overloaded exception
    // They just take very long to finish
    MetricsRecordBuilder builder = getMetrics(rpcMetrics.name());
    double queueTimeAvg = getDoubleGauge("RpcQueueTimeAvgTime", builder);
    double queueTimeMax = getDoubleGauge("RpcQueueTimeIMaxTime", builder);
    double processingTimeAvg = getDoubleGauge("RpcProcessingTimeAvgTime", builder);
    double processingTimeMax = getDoubleGauge("RpcProcessingTimeIMaxTime", builder);

    // First ns1 call has negligible queue time, LONG_RPC_TIME_MS processing time
    // All ns0 calls have LONG_RPC_TIME_MS queue time and negligible processing time
    // 2nd ns1 call takes 2*LONG_RPC_TIME_MS processing time
    // 3rd ns1 call takes 3*LONG_RPC_TIME_MS processing time
    assertApproximate((long) queueTimeAvg,
        SHORT_RPC_CALLS * LONG_RPC_TIME_MS / TOTAL_RPC, (float) 0.05);
    assertApproximate((long) processingTimeAvg,
        LONG_RPC_CALLS * (LONG_RPC_CALLS + 1) / 2 * LONG_RPC_TIME_MS
            / TOTAL_RPC, (float) 0.05);
    assertApproximate((long) queueTimeMax, LONG_RPC_TIME_MS, (float) 0.05);
    assertApproximate((long) processingTimeMax,
        LONG_RPC_TIME_MS * LONG_RPC_CALLS, (float) 0.05);
  }

  @Test
  public void testDeepHandlersQueueOverflow() throws Exception {
    int permitWaitTimeMs = 30;
    overloadedExceptionCaught = new AtomicInteger(0);
    setupCluster(true, permitWaitTimeMs);

    List<Thread> longThreads = new ArrayList<>();

    DFSClient routerClient = new DFSClient(routerContext.getFileSystemURI(),
        new HdfsConfiguration());
    // 1 executed by a dedicated surface handler
    // All calls in the queue can stay in the queue
    // All calls in can be handled by LONG_HANDLER_LIMIT are handled by deep handlers
    // 1 more call is held and blocked by DeepQueueWatcher
    // Hence the "1 + DEEP_QUEUE_CAPACITY + LONG_HANDLER_LIMIT + 1" part
    // Any calls after this are discarded and thrown back to client
    int expectedFailedCalls = 5;

    queueThread("ns1", routerClient,
        1 + DEEP_QUEUE_CAPACITY + LONG_HANDLER_LIMIT + 1 + expectedFailedCalls,
        longThreads);

    for (Thread thread : longThreads) {
      thread.start();
    }

    for (Thread thread : longThreads) {
      thread.join();
    }

    assertEquals(expectedFailedCalls, overloadedExceptionCaught.get());
  }

  /**
   * Executes test then parses log for rpc details
   */
  private void runTest(long delay) throws IOException, InterruptedException {
    overloadedExceptionCaught = new AtomicInteger(0);

    List<Thread> longThreads = new ArrayList<>();
    List<Thread> shortThreads = new ArrayList<>();

    DFSClient routerClient = new DFSClient(routerContext.getFileSystemURI(),
        new HdfsConfiguration());
    queueThread("ns1", routerClient, LONG_RPC_CALLS, longThreads);
    queueThread("ns0", routerClient, SHORT_RPC_CALLS, shortThreads);

    // Start 3 long blocking ns1 threads that hog up all handlers
    for (Thread thread : longThreads) {
      thread.start();
    }
    // Sleep a bit to make sure all ns1 call threads are up and running
    Thread.sleep(delay);

    // Start all normal threads for calls to ns0.
    for (Thread thread : shortThreads) {
      thread.start();
    }

    for (Thread thread : longThreads) {
      thread.join();
    }
    for (Thread thread : shortThreads) {
      thread.join();
    }
  }

  private void queueThread(final String dest, final DFSClient routerClient,
      int numOps, List<Thread> threads) {
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
      Thread.UncaughtExceptionHandler h =
          new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread th, Throwable ex) {
              if (ex instanceof RuntimeException
                  && ex.getCause() instanceof RemoteException
                  && ((RemoteException) ex.getCause()).getClassName().equals(
                  OverloadedNameserviceException.class.getCanonicalName())) {
                overloadedExceptionCaught.incrementAndGet();
              }
            }
          };
      thread.setUncaughtExceptionHandler(h);
      threads.add(thread);
    }
  }

  private void createMountTableEntry(final String mountPoint,
      final String[] targets) throws Exception {

    RouterClient admin = routerContext.getAdminClient();
    MountTableManager mountTable = admin.getMountTableManager();
    Map<String, String> destMap = new HashMap<>();
    for (String target : targets) {
      destMap.put(target, mountPoint);
    }
    MountTable newEntry = MountTable.newInstance(mountPoint, destMap);
    newEntry.setDestOrder(DestinationOrder.HASH);
    AddMountTableEntryRequest addRequest =
        AddMountTableEntryRequest.newInstance(newEntry);
    AddMountTableEntryResponse addResponse =
        mountTable.addMountTableEntry(addRequest);
    boolean created = addResponse.getStatus();
    assertTrue(created);

    // Refresh the caches to get the mount table
    Router router = routerContext.getRouter();
    StateStoreService stateStore = router.getStateStore();
    stateStore.refreshCaches(true);
  }

  private void assertApproximate(long tester, long target, float epsilon) {
    float ratio = (float) tester / target;
    assertTrue(String.format(
        "Value %s is outside expected range: target=%s, epsilon=%s", tester,
        target, epsilon), 1 - epsilon < ratio && ratio < 1 + epsilon);
  }
}
