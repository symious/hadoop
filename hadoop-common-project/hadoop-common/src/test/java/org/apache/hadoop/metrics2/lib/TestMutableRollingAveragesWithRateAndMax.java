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
package org.apache.hadoop.metrics2.lib;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Supplier;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.hadoop.test.MetricsAsserts.getDoubleGauge;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;

public class TestMutableRollingAveragesWithRateAndMax {

  @Test(timeout = 30000)
  public void testMutableRollingAveragesWithRate() throws Exception {
    final DummyTestMetric testMetric = new DummyTestMetric();
    testMetric.create();

    testMetric.add("metric1", 100);
    testMetric.add("metric1", 900);
    testMetric.add("metric2", 900);
    testMetric.add("metric2", 1000);
    testMetric.add("metric2", 1100);

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        testMetric.collectThreadLocalStates();
        return testMetric.getStats().size() > 0;
      }
    }, 500, 5000);

    MetricsRecordBuilder rb = getMetrics(DummyTestMetric.METRIC_NAME);

    double metric1Avg = getDoubleGauge("[Metric1]RollingAvgTesting", rb);
    double metric2Avg = getDoubleGauge("[Metric2]RollingAvgTesting", rb);
    double metric1Max = getDoubleGauge("[Metric1]MaxTesting", rb);
    double metric2Max = getDoubleGauge("[Metric2]MaxTesting", rb);
    double metric1Rate = getDoubleGauge("[Metric1]RateTesting", rb);
    double metric2Rate = getDoubleGauge("[Metric2]RateTesting", rb);
    Assert.assertEquals("The rolling average of metric1 is not as expected",
        500.0, metric1Avg, 0.0);
    Assert.assertEquals("The rolling average of metric2 is not as expected",
        1000.0, metric2Avg, 0.0);
    Assert.assertEquals("The rolling max of metric1 is not as expected",
        900.0, metric1Max, 0.0);
    Assert.assertEquals("The rolling max of metric2 is not as expected",
        1100.0, metric2Max, 0.0);
    Assert.assertEquals("The rate of metric1 is not as expected",
        2.0 / 36 / 300, metric1Rate, 1E-7);
    Assert.assertEquals("The rate of metric2 is not as expected",
        3.0 / 36 / 300, metric2Rate, 1E-7);
  }

  class DummyTestMetric {
    @Metric (valueName = "testing")
    private MutableRollingAveragesWithRateAndMax rollingAverages;

    static final String METRIC_NAME = "RollingAveragesTestMetric";

    protected void create() {
      DefaultMetricsSystem.instance().register(METRIC_NAME,
          "mutable rolling averages test", this);
      rollingAverages.replaceScheduledTask(10, 1000, TimeUnit.MILLISECONDS);
    }

    void add(String name, long latency) {
      rollingAverages.add(name, latency);
    }

    void collectThreadLocalStates() {
      rollingAverages.collectThreadLocalStates();
    }

    Map<String, Double> getStats() {
      return rollingAverages.getStats(0);
    }

  }
}
