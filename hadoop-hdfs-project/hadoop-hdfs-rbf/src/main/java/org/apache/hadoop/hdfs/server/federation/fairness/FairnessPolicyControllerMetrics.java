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

package org.apache.hadoop.hdfs.server.federation.fairness;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.util.MutableMetricRegister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metrics related to the fairness controllers
 */
@Metrics(name = "FairnessPolicyController", about = "Router RPC Fairness Policy Controller", context = "dfs")
public class FairnessPolicyControllerMetrics {
  private static final Logger LOG = LoggerFactory.getLogger(FairnessPolicyControllerMetrics.class);
  private final MetricsRegistry registry = new MetricsRegistry("routerfairnesscontroller");
  private final RouterRpcFairnessPolicyController controller;
  private final ConcurrentHashMap<String, MutableCounterLong> nsPermitAttempts;
  private final ConcurrentHashMap<String, MutableRate> nsPermitWaitTimes;
  private final ConcurrentHashMap<String, MutableRate> nsPermitHoldTimes;

  @Metric("Number of permit acquisition attempts that failed due to missing permit manager")
  private MutableCounterLong missingPermitOp;

  public FairnessPolicyControllerMetrics(RouterRpcFairnessPolicyController controller) {
    this.controller = controller;
    this.nsPermitAttempts = new ConcurrentHashMap<>();
    this.nsPermitWaitTimes = new ConcurrentHashMap<>();
    this.nsPermitHoldTimes = new ConcurrentHashMap<>();
  }

  public static FairnessPolicyControllerMetrics create(
      RouterRpcFairnessPolicyController controller) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    // Deregister existing metrics. Used during controller refresh
    ms.unregisterSource(FairnessPolicyControllerMetrics.class.getName());
    return ms.register(FairnessPolicyControllerMetrics.class.getName(),
        "Router Fairness Policy Controller Metrics",
        new FairnessPolicyControllerMetrics(controller));
  }

  public MetricsRegistry getRegistry() {
    return registry;
  }

  public void addPermitWaitTime(long time, String nsId) {
    MutableMetricRegister.tryGetMetric(registry, nsId, nsPermitWaitTimes, "PermitWaitTime_",
        MutableRate.class).add(time);
    MutableMetricRegister.tryGetMetric(registry, nsId, nsPermitAttempts, "PermitAttempts_",
        MutableCounterLong.class).incr();
  }

  public void addPermitHoldTime(long time, String nsId) {
    MutableMetricRegister.tryGetMetric(registry, nsId, nsPermitHoldTimes, "PermitHoldTime_",
        MutableRate.class).add(time);
  }

  public void incrMissingPermit() {
    missingPermitOp.incr();
  }
}
