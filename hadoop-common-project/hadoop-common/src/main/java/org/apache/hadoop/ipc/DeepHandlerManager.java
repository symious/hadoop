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

package org.apache.hadoop.ipc;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.metrics.DeepRpcMetrics;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.htrace.core.TraceScope;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.CommonConfigurationKeys.DFS_ROUTER_DEEP_HANDLER_MAX_UTILIZATION_PERCENTAGE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeys.DFS_ROUTER_DEEP_HANDLER_MAX_UTILIZATION_PERCENTAGE_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeys.DFS_ROUTER_DEEP_QUEUE_CAPACITY_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeys.DFS_ROUTER_DEEP_QUEUE_CAPACITY_KEY;
import static org.apache.hadoop.ipc.Server.getClientBackoffEnable;
import static org.apache.hadoop.ipc.Server.getQueueClass;
import static org.apache.hadoop.ipc.Server.getSchedulerClass;

public class DeepHandlerManager {

  public static final Logger LOG = LoggerFactory.getLogger(DeepHandlerManager.class);

  private final SurfaceHandler[] handlers;
  private final BlockingQueue<DeepHandler> deepHandlers;
  private final CallQueueManager<Server.Call> callQueue;
  private final ConcurrentMap<String, CallQueueManager<Server.Call>> deepCallQueues;
  private final ConcurrentMap<String, DeepQueueWatcher> watchers;
  private final Server server;
  private final int port;
  private final Configuration conf;
  private final AlignmentContext alignmentContext;
  private final int deepQueueCapacity;
  private final int deepHandlerUtilization;
  private final DeepRpcMetrics metrics;
  volatile private boolean running = true;

  private final ConcurrentHashMap<String, AtomicInteger> deepCallsByNamespace;

  public final static String CONTEXT_KEY = "deepQueue";

  public DeepHandlerManager(Server server, CallQueueManager<Server.Call> callQueue,
      AlignmentContext alignmentContext, int port, Configuration conf, int handlerCount) {
    this.server = server;
    this.port = port;
    this.conf = conf;
    this.callQueue = callQueue;
    this.alignmentContext = alignmentContext;
    this.metrics = DeepRpcMetrics.create(this, port);

    int deepQueueCapacity =
        conf.getInt(DFS_ROUTER_DEEP_QUEUE_CAPACITY_KEY, DFS_ROUTER_DEEP_QUEUE_CAPACITY_DEFAULT);

    if (deepQueueCapacity <= 0) {
      LOG.info("Invalid deep queue capacity {}, using default value {}.", deepQueueCapacity,
          DFS_ROUTER_DEEP_QUEUE_CAPACITY_DEFAULT);
      deepQueueCapacity = DFS_ROUTER_DEEP_QUEUE_CAPACITY_DEFAULT;
    }

    double configuredUtilization =
        conf.getDouble(DFS_ROUTER_DEEP_HANDLER_MAX_UTILIZATION_PERCENTAGE_KEY,
            DFS_ROUTER_DEEP_HANDLER_MAX_UTILIZATION_PERCENTAGE_DEFAULT);
    if (configuredUtilization <= 0 || configuredUtilization > 1) {
      LOG.info("Invalid deep handler utilization {}, using default value {}.",
          configuredUtilization, DFS_ROUTER_DEEP_HANDLER_MAX_UTILIZATION_PERCENTAGE_DEFAULT);
      configuredUtilization = DFS_ROUTER_DEEP_HANDLER_MAX_UTILIZATION_PERCENTAGE_DEFAULT;
    }

    int deepHandlerCount = conf.getInt(CommonConfigurationKeys.DFS_ROUTER_DEEP_HANDLER_COUNT_KEY,
        CommonConfigurationKeys.DFS_ROUTER_DEEP_HANDLER_COUNT_DEFAULT);
    this.deepQueueCapacity = deepQueueCapacity;
    this.deepHandlerUtilization = (int) Math.floor(configuredUtilization * deepHandlerCount);

    deepCallQueues = new ConcurrentHashMap<>();
    watchers = new ConcurrentHashMap<>();

    handlers = new SurfaceHandler[handlerCount];
    deepHandlers = new ArrayBlockingQueue<>(deepHandlerCount);

    for (int i = 0; i < handlerCount; i++) {
      handlers[i] = new SurfaceHandler(i);
      handlers[i].start();
    }
    for (int i = 0; i < deepHandlerCount; i++) {
      DeepHandler deepHandler = new DeepHandler(i);
      deepHandlers.add(deepHandler);
      deepHandler.start();
    }

    deepCallsByNamespace = new ConcurrentHashMap<>();
  }

  public String getCurrentDeepHandlerUtilization() {
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Integer> result = new HashMap<>();
    for (Map.Entry<String, DeepQueueWatcher> entry : watchers.entrySet()) {
      int utilization = entry.getValue().getCurrentUtilization();
      if (utilization > 0) {
        result.put(entry.getKey(), utilization);
      }
    }
    try {
      return mapper.writeValueAsString(result);
    } catch (IOException e) {
      LOG.warn("Failed to export deep handler metrics.");
      return null;
    }
  }

  public int getCurrentFreeDeepHandlerCount() {
    return deepHandlers.size();
  }

  public String getCurrentDeepQueueSizes() {
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Integer> result = new HashMap<>();
    for (Map.Entry<String, DeepQueueWatcher> entry : watchers.entrySet()) {
      int queueSize = entry.getValue().getQueueSize();
      result.put(entry.getKey(), queueSize);
    }
    try {
      return mapper.writeValueAsString(result);
    } catch (IOException e) {
      LOG.warn("Failed to export deep handler metrics.");
      return null;
    }
  }

  public String getDeepCallsByNamespace() {
    try {
      return new ObjectMapper().writeValueAsString(deepCallsByNamespace);
    } catch (IOException e) {
      LOG.warn("Failed to export deep handler metrics.");
      return null;
    }
  }

  private void incrDeepCall(String namespace) {
    if (!deepCallsByNamespace.containsKey(namespace)) {
      synchronized (deepCallsByNamespace) {
        if (!deepCallsByNamespace.containsKey(namespace)) {
          deepCallsByNamespace.put(namespace, new AtomicInteger(0));
        }
      }
    }
    deepCallsByNamespace.get(namespace).getAndIncrement();
  }

  @VisibleForTesting
  public DeepRpcMetrics getMetrics() {
    return metrics;
  }

  /**
   * The same as {@link Handler} but instead of throwing a StandbyException
   * back to client when overloaded (only applicable to router RPC server, not
   * namenode RPC server), it requeues the call to {@link #deepCallQueues} for
   * a {@link DeepHandler} to take care later.
   */
  private class SurfaceHandler extends Thread {

    int id;

    public SurfaceHandler(int instanceNumber) {
      this.id = instanceNumber;
      this.setDaemon(true);
      this.setName("IPC Server SurfaceHandler " + instanceNumber + " on default port " + port);
    }

    /**
     * Refer to {@link Server.Handler#requeueCall}
     */
    protected void requeueCall(Server.Call call) throws IOException, InterruptedException {
      try {
        try {
          callQueue.add(call);
          long deltaNanos = Time.monotonicNowNanos() - call.timestampNanos;
          call.getProcessingDetails()
              .set(ProcessingDetails.Timing.ENQUEUE, deltaNanos, TimeUnit.NANOSECONDS);
        } catch (CallQueueManager.CallQueueOverflowException cqe) {
          server.rpcMetrics.incrClientBackoff();
          // unwrap retriable exception.
          throw cqe.getCause();
        }
        server.rpcMetrics.incrRequeueCalls();
      } catch (RpcServerException rse) {
        call.doResponse(rse.getCause(), rse.getRpcStatusProto());
      }
    }

    @Override
    public void run() {
      LOG.debug(Thread.currentThread().getName() + ": starting");
      while (running) {
        TraceScope traceScope = null;
        Server.Call call = null;
        long startTimeNanos = 0;
        // True iff the connection for this call has been dropped.
        // Set to true by default and update to false later if the connection
        // can be successfully read.
        boolean connDropped = true;
        boolean passedToDeepHandlers = false;
        long startProcessingNanos = 0;
        try {
          call = callQueue.take(); // pop the queue; maybe blocked here
          startTimeNanos = Time.monotonicNowNanos();
          if (alignmentContext != null && call.isCallCoordinated()
              && call.getClientStateId() > alignmentContext.getLastSeenStateId()) {
            /*
             * The call processing should be postponed until the client call's
             * state id is aligned (<=) with the server state id.

             * NOTE:
             * Inserting the call back to the queue can change the order of call
             * execution comparing to their original placement into the queue.
             * This is not a problem, because Hadoop RPC does not have any
             * constraints on ordering the incoming rpc requests.
             * In case of Observer, it handles only reads, which are
             * commutative.
             */
            // Re-queue the call and continue
            requeueCall(call);
            call = null;
            continue;
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug(
                Thread.currentThread().getName() + ": " + call + " for RpcKind " + call.rpcKind);
          }
          call.setCanPassToDeepQueue(true);
          Server.getCurCall().set(call);
          if (call.getTraceScope() != null) {
            call.getTraceScope().reattach();
            traceScope = call.getTraceScope();
            traceScope.getSpan().addTimelineAnnotation("called");
          }
          // always update the current call context
          CallerContext.setCurrent(call.getCallerContext());
          UserGroupInformation remoteUser = call.getRemoteUser();
          connDropped = !call.isOpen();
          startProcessingNanos = Time.monotonicNowNanos();
          if (remoteUser != null) {
            remoteUser.doAs(call);
          } else {
            call.run();
          }
        } catch (OverloadedNameserviceException e) {
          // If call fails due to overloaded permit controllers
          // Pass to deep handlers to retry
          try {
            ((Server.RpcCall) call).markDeepQueueStartTime();
            metrics.incrDeepCalls(e.getNameservice());
            incrDeepCall(e.getNameservice());
            CallQueueManager<Server.Call> deepCallQueue = getDeepCallQueue(e.getNameservice());
            // Do not block, fail immediately if queue full
            deepCallQueue.add(call);
            resetCallMetricsForDeepHandler(call);
            LOG.debug("{}: Router overloaded for NS {}, putting {} in deep queue",
                Thread.currentThread().getName(), e.getNameservice(), call);
            passedToDeepHandlers = true;
          } catch (CallQueueManager.CallQueueOverflowException cqoe) {
            // Throw an OverloadedNameserviceException
            // back to client if failed from full queue
            metrics.incrRejectedDeepCalls(e.getNameservice());
            String msg =
                "Router " + e.getRouterId() + " is overloaded for NS: " + e.getNameservice();
            OverloadedNameserviceException resException =
                new OverloadedNameserviceException(msg, e.getRouterId(), e.getNameservice());
            try {
              ((Server.RpcCall) call).sendOnlyException(resException, startProcessingNanos);
            } catch (IOException ex) {
              LOG.info(
                  Thread.currentThread().getName() + " failed to send exception back to client",
                  cqoe);
              throw new RuntimeException(ex);
            }
          }
        } catch (InterruptedException e) {
          if (running) {                          // unexpected -- log it
            LOG.info(Thread.currentThread().getName() + " unexpectedly interrupted", e);
            if (traceScope != null) {
              traceScope.getSpan().addTimelineAnnotation(
                  "unexpectedly interrupted: " + StringUtils.stringifyException(e));
            }
          }
        } catch (Exception e) {
          LOG.info(Thread.currentThread().getName() + " caught an exception", e);
          if (traceScope != null) {
            traceScope.getSpan()
                .addTimelineAnnotation("Exception: " + StringUtils.stringifyException(e));
          }
        } finally {
          Server.getCurCall().set(null);
          if (call != null && !passedToDeepHandlers) {
            IOUtils.cleanupWithLogger(LOG, traceScope);
            server.updateMetrics(call, startTimeNanos, connDropped, id);
            ProcessingDetails.LOG.debug("Served: [{}]{} name={} user={} details={}", call,
                (call.isResponseDeferred() ? ", deferred" : ""), call.getDetailedMetricsName(),
                call.getRemoteUser(), call.getProcessingDetails());
          }
        }
      }
      LOG.debug(Thread.currentThread().getName() + ": exiting");
    }

    /**
     * Reset all existing metrics except ENQUEUE when the call was in a shallow handler.
     * Assign time spent in shallow handler to QUEUE.
     */
    private void resetCallMetricsForDeepHandler(Server.Call call) {
      for (ProcessingDetails.Timing type : ProcessingDetails.Timing.values()) {
        if (type.equals(ProcessingDetails.Timing.ENQUEUE)) {
          continue;
        }
        call.getProcessingDetails().set(type, 0);
      }
    }

    private CallQueueManager<Server.Call> getDeepCallQueue(String ns) {
      if (deepCallQueues.containsKey(ns)) {
        return deepCallQueues.get(ns);
      }
      synchronized (deepCallQueues) {
        if (deepCallQueues.containsKey(ns)) {
          return deepCallQueues.get(ns);
        } else {
          String prefix = CommonConfigurationKeys.IPC_NAMESPACE + "." + port;
          CallQueueManager<Server.Call> newQueue =
              new CallQueueManager<>(getQueueClass(prefix, conf), getSchedulerClass(prefix, conf),
                  getClientBackoffEnable(prefix, conf), deepQueueCapacity, prefix, conf);
          deepCallQueues.put(ns, newQueue);
          DeepQueueWatcher watcher = new DeepQueueWatcher(ns, deepHandlerUtilization, newQueue);
          watchers.put(ns, watcher);
          watcher.start();
        }
      }
      return deepCallQueues.get(ns);
    }
  }

  /**
   * Class to monitor {@link #deepCallQueues}, pull calls from the queue,
   * pass to corresponding {@link DeepHandler} to handle.
   * Should only spawn one.
   */
  private class DeepQueueWatcher extends Thread {
    private final CallQueueManager<Server.Call> callQueue;
    private final Semaphore freeHandler;
    private final int maxUtilization;
    private final String nameservice;

    DeepQueueWatcher(String nameservice, int maxUtilization,
        CallQueueManager<Server.Call> callQueue) {
      this.nameservice = nameservice;
      this.maxUtilization = maxUtilization;
      this.freeHandler = new Semaphore(maxUtilization);
      this.callQueue = callQueue;
    }

    public int getCurrentUtilization() {
      return this.maxUtilization - this.freeHandler.availablePermits();
    }

    public int getQueueSize() {
      return this.callQueue.size();
    }

    public String getNameservice() {
      return nameservice;
    }

    void releaseHandler() {
      this.freeHandler.release();
    }

    @Override
    public void run() {
      while (running) {
        try {
          // Try to acquire a call from the deep queue, will block until possible
          Server.Call call = callQueue.take();
          this.freeHandler.acquire();
          DeepHandler handler = deepHandlers.take();
          handler.handleCall(this, callQueue, call);
        } catch (InterruptedException e) {
          if (running) {
            LOG.info(Thread.currentThread().getName() + " unexpectedly interrupted", e);
          }
        }
      }
    }
  }

  /**
   * The same as {@link Handler} but only handles a call passed by {@link DeepQueueWatcher}
   * from {@link #deepCallQueues} instead of polling {@link #callQueue}.
   */
  private class DeepHandler extends SurfaceHandler {
    private final SynchronousQueue<Server.Call> currentCall = new SynchronousQueue<>();
    CallQueueManager<Server.Call> currentCallQueue = null;
    private DeepQueueWatcher currentWatcher;

    public DeepHandler(int instanceNumber) {
      super(instanceNumber);
      this.setName("IPC Server DeepHandler " + instanceNumber + " on default port " + port);
    }

    public void handleCall(DeepQueueWatcher watcher, CallQueueManager<Server.Call> callQueue,
        Server.Call nextCall) {
      this.currentWatcher = watcher;
      this.currentCallQueue = callQueue;
      currentCall.offer(nextCall);
    }

    @Override
    public void run() {
      LOG.debug(Thread.currentThread().getName() + ": starting");
      while (running) {
        TraceScope traceScope = null;
        Server.Call call = null;
        long startTimeNanos = 0;
        // True iff the connection for this call has been dropped.
        // Set to true by default and update to false later if the connection
        // can be succesfully read.
        boolean connDropped = true;

        try {
          call = currentCall.take();
          startTimeNanos = Time.monotonicNowNanos();
          LOG.debug("{}: Pulled call {} from deep handler queue at {}",
              Thread.currentThread().getName(), call, startTimeNanos);
          // Skip the fancy processing since this is an accepted call
          // but failed due to overloaded permit controller
          call.setCanPassToDeepQueue(false);
          Server.getCurCall().set(call);
          if (call.getTraceScope() != null) {
            call.getTraceScope().reattach();
            traceScope = call.getTraceScope();
            traceScope.getSpan().addTimelineAnnotation("recalled");
          }
          // always update the current call context
          CallerContext.setCurrent(call.getCallerContext());
          UserGroupInformation remoteUser = call.getRemoteUser();
          connDropped = !call.isOpen();
          if (remoteUser != null) {
            remoteUser.doAs(call);
          } else {
            call.run();
          }
        } catch (InterruptedException e) {
          if (running) {                          // unexpected -- log it
            LOG.info(Thread.currentThread().getName() + " unexpectedly interrupted", e);
            if (traceScope != null) {
              traceScope.getSpan().addTimelineAnnotation(
                  "unexpectedly interrupted: " + StringUtils.stringifyException(e));
            }
          }
        } catch (Exception e) {
          LOG.info(Thread.currentThread().getName() + " caught an exception", e);
          if (traceScope != null) {
            traceScope.getSpan()
                .addTimelineAnnotation("Exception: " + StringUtils.stringifyException(e));
          }
        } finally {
          Server.getCurCall().set(null);
          IOUtils.cleanupWithLogger(LOG, traceScope);
          if (call != null) {
            if (currentCallQueue != null) {
              server.updateMetricsInternal(call, startTimeNanos, connDropped, id, currentCallQueue);
              metrics.addDeepHandlerProcessingTime(
                  (Time.monotonicNowNanos() - startTimeNanos) / 1000000,
                  this.currentWatcher.getNameservice());
              metrics.addDeepLatency(
                  (Time.monotonicNowNanos() - ((Server.RpcCall) call).getDeepQueueStartTime())
                      / 1000000, this.currentWatcher.getNameservice());
              currentCallQueue = null;
            }
            if (currentWatcher != null) {
              currentWatcher.releaseHandler();
            }
            ProcessingDetails.LOG.debug("Served: [{}]{} name={} user={} details={}", call,
                (call.isResponseDeferred() ? ", deferred" : ""), call.getDetailedMetricsName(),
                call.getRemoteUser(), call.getProcessingDetails());
            // Plug current handler back into the queue of available handlers
            // after dealing with the call
            deepHandlers.add(this);
          }
        }
      }
      LOG.debug(Thread.currentThread().getName() + ": exiting");
    }
  }

  public synchronized void shutdown() {
    running = false;
    for (SurfaceHandler s : handlers) {
      if (s != null) {
        s.interrupt();
      }
    }
    for (DeepHandler d : deepHandlers) {
      if (d != null) {
        d.interrupt();
      }
    }
    for (DeepQueueWatcher watcher : watchers.values()) {
      watcher.interrupt();
    }
  }
}
