/*
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
package org.apache.hadoop.yarn.server.globalpolicygenerator;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.metrics2.lib.Interns.info;

/**
 * This class is for maintaining the router app meta changes statistics and
 * publishing them through the metrics interfaces.
 */
@InterfaceAudience.Private
@Metrics(about = "Metrics for GPG", context = "gpg")
public final class GPGMetrics {

  private static final MetricsInfo RECORD_INFO =
      info("GPGMetrics", "GPGMetrics");
  private static AtomicBoolean isInitialized = new AtomicBoolean(false);

  @Metric("# recent sum app meta numbers")
  private MutableGaugeLong sumAppStateStores;

  @Metric("# recent deleted app meta numbers")
  private MutableGaugeLong deletedAppStateStores;

  private static volatile GPGMetrics INSTANCE = null;
  private static MetricsRegistry registry;

  private GPGMetrics() {
    registry = new MetricsRegistry(RECORD_INFO);
    registry.tag(RECORD_INFO, "GPG");
  }

  public static GPGMetrics getMetrics() {
    if (!isInitialized.get()) {
      synchronized (GPGMetrics.class) {
        if (INSTANCE == null) {
          INSTANCE = DefaultMetricsSystem.instance().register("GPGMetrics",
              "Metrics for the Yarn GPG", new GPGMetrics());
          isInitialized.set(true);
        }
      }
    }
    return INSTANCE;
  }

  @VisibleForTesting
  synchronized static void destroy() {
    isInitialized.set(false);
    INSTANCE = null;
  }

  @VisibleForTesting
  public long getSumAppStateStores() {
    return sumAppStateStores.value();
  }

  @VisibleForTesting
  public long getDeletedAppStateStores() {
    return deletedAppStateStores.value();
  }

  public void incrSumAppStateStores(long sumAppStates) {
    sumAppStateStores.incr(sumAppStates);
  }

  public void incrDeletedAppStateStores(long deletedAppStates) {
    deletedAppStateStores.incr(deletedAppStates);
  }

}
