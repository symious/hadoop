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

package org.apache.hadoop.ipc.metrics;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.ipc.DeepHandlerManager;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.util.MutableMetricRegister;

@Metrics(about = "Deep layer RPC metrics", context = "rpc")
public class DeepRpcMetrics {

  private final DeepHandlerManager manager;
  private final MetricsRegistry registry;
  private final String name;

  public DeepRpcMetrics(DeepHandlerManager dhm, int port) {
    this.manager = dhm;
    registry = new MetricsRegistry("deeprpc").tag("port", "RPC port", String.valueOf(port));
    nsDeepCalls = new ConcurrentHashMap<>();
    nsRejectedDeepCalls = new ConcurrentHashMap<>();
    nsDeepHandlerProcessingTime = new ConcurrentHashMap<>();
    nsLatencies = new ConcurrentHashMap<>();
    name = DeepRpcMetrics.class.getName() + port;
  }

  public static DeepRpcMetrics create(DeepHandlerManager dhm, int port) {
    DeepRpcMetrics m = new DeepRpcMetrics(dhm, port);
    return DefaultMetricsSystem.instance().register(m.name, null, m);
  }

  public String getName() {
    return name;
  }

  @Metric("Number of calls that went to deep handlers")
  MutableCounterLong deepCalls;
  @Metric("Number of calls that got rejected by deep handlers")
  MutableCounterLong rejectedDeepCalls;
  @Metric("Processing time in deep handlers")
  MutableRate deepHandlerProcessingTime;
  @Metric("Latency compared to calls directly handled in the shallow layer")
  MutableRate deepLatency;  // Latency = processing time + wait time

  private final ConcurrentHashMap<String, MutableCounterLong> nsDeepCalls;
  private final ConcurrentHashMap<String, MutableCounterLong> nsRejectedDeepCalls;
  private final ConcurrentHashMap<String, MutableRate> nsDeepHandlerProcessingTime;
  private final ConcurrentHashMap<String, MutableRate> nsLatencies;

  @Metric("Current deep queue size per namespace")
  public String getCurrentDeepQueueSizes() {
    return manager.getCurrentDeepQueueSizes();
  }

  @Metric("Number of deep calls requested per namespace")
  public String getDeepCallsByNamespace() {
    return manager.getDeepCallsByNamespace();
  }

  @Metric("Current utilized deep handler count per namespace")
  public String getCurrentDeepHandlerUtilization() {
    return manager.getCurrentDeepHandlerUtilization();
  }

  @Metric("Current free deep handler count")
  public int getCurrentFreeDeepHandlerCount() {
    return manager.getCurrentFreeDeepHandlerCount();
  }

  public void incrDeepCalls(String nsId) {
    deepCalls.incr();
    MutableMetricRegister.tryGetMetric(registry, nsId, nsDeepCalls, "DeepCallAttempts_",
        MutableCounterLong.class).incr();
  }

  public void incrRejectedDeepCalls(String nsId) {
    rejectedDeepCalls.incr();
    MutableMetricRegister.tryGetMetric(registry, nsId, nsRejectedDeepCalls, "DeepCallsRejected_",
        MutableCounterLong.class).incr();
  }

  public void addDeepHandlerProcessingTime(long processingTime, String nsId) {
    deepHandlerProcessingTime.add(processingTime);
    MutableMetricRegister.tryGetMetric(registry, nsId, nsDeepHandlerProcessingTime,
        "DeepCallProcessTime_", MutableRate.class).add(processingTime);
  }

  public void addDeepLatency(long latency, String nsId) {
    deepLatency.add(latency);
    MutableMetricRegister.tryGetMetric(registry, nsId, nsLatencies, "DeepCallLatency_",
        MutableRate.class).add(latency);
  }
}
