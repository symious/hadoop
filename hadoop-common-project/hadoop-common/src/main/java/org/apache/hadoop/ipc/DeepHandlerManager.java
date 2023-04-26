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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.htrace.core.TraceScope;
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
  volatile private boolean running = true;

  public final static String CONTEXT_KEY = "deepQueue";

  public DeepHandlerManager(Server server, CallQueueManager<Server.Call> callQueue,
                            AlignmentContext alignmentContext, int port, Configuration conf, int handlerCount) {
    this.server = server;
    this.port = port;
    this.conf = conf;
    this.callQueue = callQueue;
    this.alignmentContext = alignmentContext;

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
  }

  private class SurfaceHandler extends Thread {
    int id;
    public SurfaceHandler(int instanceNumber) {
      this.id = instanceNumber;
      this.setDaemon(true);
      this.setName("IPC Server SurfaceHandler " + instanceNumber + " on default port " + port);
    }

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
          if (remoteUser != null) {
            remoteUser.doAs(call);
          } else {
            call.run();
          }
        } catch (OverloadedNameserviceException e) {
          // If call fails due to overloaded permit controllers
          // Pass to deep handlers to retry
          try {
            CallQueueManager<Server.Call> deepCallQueue = getDeepCallQueue(e.getNameservice());
            // Do not block, fail immediately if queue full
            deepCallQueue.add(call);
            LOG.debug("{}: Router overloaded for NS {}, putting {} in deep queue",
                Thread.currentThread().getName(), e.getNameservice(), call);
            passedToDeepHandlers = true;
          } catch (CallQueueManager.CallQueueOverflowException cqoe) {
            // Throw an OverloadedNameserviceException
            // back to client if failed from full queue
            String msg =
                "Router " + e.getRouterId() + " is overloaded for NS: " + e.getNameservice();
            OverloadedNameserviceException resException =
                new OverloadedNameserviceException(msg, e.getRouterId(), e.getNameservice());
            try {
              ((Server.RpcCall) call).sendOnlyException(resException);
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
            server.updateMetrics(call, startTimeNanos, connDropped);
            ProcessingDetails.LOG.debug("Served: [{}]{} name={} user={} details={}", call,
                (call.isResponseDeferred() ? ", deferred" : ""), call.getDetailedMetricsName(),
                call.getRemoteUser(), call.getProcessingDetails());
          }
        }
      }
      LOG.debug(Thread.currentThread().getName() + ": exiting");
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
          DeepQueueWatcher watcher = new DeepQueueWatcher(deepHandlerUtilization, newQueue);
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

    DeepQueueWatcher(int maxUtilization, CallQueueManager<Server.Call> callQueue) {
      this.freeHandler = new Semaphore(maxUtilization);
      this.callQueue = callQueue;
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
              server.updateMetricsInternal(call, startTimeNanos, connDropped, currentCallQueue);
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
