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

import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.metrics2.lib.MutableStat;
import org.apache.hadoop.util.MutableMetricRegister;

/**
 * Metrics related to the fairness controllers
 */
@Metrics(name = "FairnessPolicyController", about = "Router RPC Fairness Policy Controller", context = "dfs")
public class FairnessPolicyControllerMetrics {
  private final MetricsRegistry registry = new MetricsRegistry("routerfairnesscontroller");
  private final ConcurrentHashMap<String, MutableStat> nsDedicatedPermitUsage;
  private final ConcurrentHashMap<String, MutableStat> nsSharedPermitUsage;

  @Metric("Number of permit acquisition attempts that failed due to missing permit manager")
  private MutableCounterLong missingPermitOp;

  public FairnessPolicyControllerMetrics() {
    this.nsDedicatedPermitUsage = new ConcurrentHashMap<>();
    this.nsSharedPermitUsage = new ConcurrentHashMap<>();
  }

  public static FairnessPolicyControllerMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    // Deregister existing metrics. Used during controller refresh
    ms.unregisterSource(FairnessPolicyControllerMetrics.class.getName());
    return ms.register(FairnessPolicyControllerMetrics.class.getName(),
        "Router Fairness Policy Controller Metrics",
        new FairnessPolicyControllerMetrics());
  }

  public MetricsRegistry getRegistry() {
    return registry;
  }

  public void addDedicatedPermitUsage(long dedicatedPermitUsage, String nsId) {
    MutableMetricRegister.tryGetMetric(registry, nsId, nsDedicatedPermitUsage,
        "DedicatedPermitUsage_", MutableStat.class).add(dedicatedPermitUsage);
  }

  public void addSharedPermitUsage(long sharedPermitUsage, String nsId) {
    MutableMetricRegister.tryGetMetric(registry, nsId, nsSharedPermitUsage,
        "SharedPermitUsage_", MutableStat.class).add(sharedPermitUsage);
  }

  public void incrMissingPermit() {
    missingPermitOp.incr();
  }
}
