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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.ipc.DeepHandlerManager;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.util.MutableMetricRegister;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;

@Metrics(about = "Deep layer RPC metrics", context = "rpc")
public class DeepRpcMetrics implements DeepRpcMetricsMBean {
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

  @Override
  public int getCurrentFreeDeepHandlerCount() {
    return manager.getCurrentFreeDeepHandlerCount();
  }

  @Override
  public CompositeData getCurrentDeepQueueSizes() {
    return convertStringIntMapToCompositeData(manager.getCurrentDeepQueueSizes());
  }

  @Override
  public CompositeData getDeepCallsByNamespace() {
    return convertStringIntMapToCompositeData(manager.getDeepCallsByNamespace());
  }

  @Override
  public CompositeData getCurrentDeepHandlerUtilization() {
    return convertStringIntMapToCompositeData(manager.getCurrentDeepHandlerUtilization());
  }

  private CompositeData convertStringIntMapToCompositeData(Map<String, Integer> input) {
    if (input.isEmpty()) {
      return null;
    }

    try {
      int size = input.size();
      String[] fields = input.keySet().toArray(new String[0]);
      OpenType[] types = Collections.nCopies(size, SimpleType.INTEGER).toArray(new OpenType[0]);
      Integer[] values = new Integer[size];
      for (int i = 0; i < size; i++) {
        values[i] = input.get(fields[i]);
      }

      CompositeType type = new CompositeType(this.getClass().getName(),
          this.getClass().getName(), fields, fields, types);
      return new CompositeDataSupport(type, fields, values);
    } catch (OpenDataException e) {
      return null;
    }
  }

  public void incrDeepCalls(String nsId) {
    deepCalls.incr();
    MutableMetricRegister.tryGetMetric(registry, nsId, nsDeepCalls, "DeepCallAttempts_",
        MutableCounterLong.class).incr();
  }

  public void incrRejectedDeepCalls(String nsId) {
    rejectedDeepCalls.incr();
    MutableMetricRegister.tryGetMetric(registry, nsId, nsDeepCalls, "DeepCallsRejected_",
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
