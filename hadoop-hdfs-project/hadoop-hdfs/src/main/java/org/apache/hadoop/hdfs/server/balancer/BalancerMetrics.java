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
package org.apache.hadoop.hdfs.server.balancer;

import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.util.MutableMetricRegister;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Metrics for Balancer.
 */
@Metrics(about="Balancer metrics", context="dfs")
final class BalancerMetrics {

  private final MetricsRegistry registry;
  private final String name;

  @Metric("Bytes moved")
  private MutableCounterLong bytesMoved;

  @Metric("Number blocks moved")
  private MutableCounterLong numBlocksMoved;

  @Metric("Number blocks fail moved")
  private MutableCounterLong numBlocksFailMoved;

  private final ConcurrentHashMap<String, MutableCounterLong> bytesMovedPerNS =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, MutableCounterLong> numBlocksMovedPerNS =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, MutableCounterLong> numBlocksFailMovedPerNS =
      new ConcurrentHashMap<>();

  private final ConcurrentHashMap<String, MutableGaugeInt> iterateRunningPerNS =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, MutableGaugeLong> bytesLeftToMovePerNS =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, MutableGaugeInt> numOfUnderUtilizedNodesPerNS =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, MutableGaugeInt> numOfOverUtilizedNodesPerNS =
      new ConcurrentHashMap<>();


  private final ConcurrentHashMap<String, MutableGaugeLong> byteMovedInPreIterPerNS =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, MutableGaugeLong> numBlocksMovedInPreIterPerNS =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, MutableGaugeLong> timeCostOfPreIterPerNS =
      new ConcurrentHashMap<>();

  public BalancerMetrics() {
    name = "BalanceMetrics";
    registry = new MetricsRegistry("BalanceMetrics");
  }

  public static BalancerMetrics create() {
    BalancerMetrics m = new BalancerMetrics();
    return DefaultMetricsSystem.instance().register(m.name, null, m);
  }

  /**
   * Increase the byte moved.
   */
  public void incrBytesMoved(long moved, String nsId) {
    bytesMoved.incr(moved);

    if (nsId == null) {
      return;
    }
    MutableCounterLong mutableCounterLong = MutableMetricRegister.tryGetMetric(registry,
        nsId, bytesMovedPerNS, "BytesMoved_", MutableCounterLong.class);
    if (mutableCounterLong != null) {
      mutableCounterLong.incr(moved);
    }
  }

  /**
   * Increase the number blocks
   */
  public void incrNumBlocksMoved(int numBlocks, String nsId) {
    numBlocksMoved.incr(numBlocks);

    if (nsId == null) {
      return;
    }
    MutableCounterLong mutableCounterLong = MutableMetricRegister.tryGetMetric(registry,
        nsId, numBlocksMovedPerNS, "NumBlocksMoved_", MutableCounterLong.class);
    if (mutableCounterLong != null) {
      mutableCounterLong.incr(numBlocks);
    }
  }

  /**
   * Increase the number blocks that failed to move.
   */
  public void incrNumBlocksFailMoved(int numBlocks, String nsId) {
    numBlocksFailMoved.incr(numBlocks);

    if (nsId == null) {
      return;
    }
    MutableCounterLong mutableCounterLong = MutableMetricRegister.tryGetMetric(registry,
        nsId, numBlocksFailMovedPerNS, "NumBlocksFailMoved_", MutableCounterLong.class);
    if (mutableCounterLong != null) {
      mutableCounterLong.incr(numBlocks);
    }
  }


  public void setIterateRunning(boolean iterateRunning, String nsId) {
    if (nsId == null) {
      return;
    }
    MutableGaugeInt mutableGaugeInt = MutableMetricRegister.tryGetMetric(registry,
        nsId, iterateRunningPerNS, "IterateRunning_", MutableGaugeInt.class);
    if (mutableGaugeInt != null) {
      mutableGaugeInt.set(iterateRunning ? 1 : 0);
    }
  }

  public void setBytesLeftToMove(long bytesLeftToMove, String nsId) {
    if (nsId == null) {
      return;
    }
    MutableGaugeLong mutableGaugeLong = MutableMetricRegister.tryGetMetric(registry,
        nsId, bytesLeftToMovePerNS, "BytesLeftToMove_", MutableGaugeLong.class);
    if (mutableGaugeLong != null) {
      mutableGaugeLong.set(bytesLeftToMove);
    }
  }

  public void setNumOfUnderUtilizedNodes(int numOfUnderUtilizedNodes, String nsId) {
    if (nsId == null) {
      return;
    }
    MutableGaugeInt mutableGaugeInt = MutableMetricRegister.tryGetMetric(registry,
        nsId, numOfUnderUtilizedNodesPerNS, "NumOfUnderUtilizedNodes_",
        MutableGaugeInt.class);
    if (mutableGaugeInt != null) {
      mutableGaugeInt.set(numOfUnderUtilizedNodes);
    }
  }

  void setNumOfOverUtilizedNodes(int numOfOverUtilizedNodes, String nsId) {
    if (nsId == null) {
      return;
    }
    MutableGaugeInt mutableGaugeInt = MutableMetricRegister.tryGetMetric(registry,
        nsId, numOfOverUtilizedNodesPerNS, "NumOfOverUtilizedNodes_",
        MutableGaugeInt.class);
    if (mutableGaugeInt != null) {
      mutableGaugeInt.set(numOfOverUtilizedNodes);
    }
  }


  void setBytesMovedInPreIter(long bytesMoved, String nsId) {
    if (nsId == null) {
      return;
    }
    MutableGaugeLong mutableGaugeLong = MutableMetricRegister.tryGetMetric(registry,
        nsId, byteMovedInPreIterPerNS, "BytesMovedInPreIter_", MutableGaugeLong.class);
    if (mutableGaugeLong != null) {
      mutableGaugeLong.set(bytesMoved);
    }
  }

  void setNumBlocksMovedInPreIter(long numBlocks, String nsId) {
    if (nsId == null) {
      return;
    }
    MutableGaugeLong mutableGaugeLong = MutableMetricRegister.tryGetMetric(registry,
        nsId, numBlocksMovedInPreIterPerNS, "NumBlocksMovedInPreIter_",
        MutableGaugeLong.class);
    if (mutableGaugeLong != null) {
      mutableGaugeLong.set(numBlocks);
    }
  }

  void setTimeCostOfPreIter(long duration, String nsId) {
    if (nsId == null) {
      return;
    }
    MutableGaugeLong mutableGaugeLong = MutableMetricRegister.tryGetMetric(registry,
        nsId, timeCostOfPreIterPerNS, "TimeCostOfPreIter_", MutableGaugeLong.class);
    if (mutableGaugeLong != null) {
      mutableGaugeLong.set(duration);
    }
  }
}