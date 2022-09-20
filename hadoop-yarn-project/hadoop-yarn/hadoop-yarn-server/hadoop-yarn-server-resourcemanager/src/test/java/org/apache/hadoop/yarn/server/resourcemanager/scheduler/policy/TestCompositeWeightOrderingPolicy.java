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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertTrue;

public class TestCompositeWeightOrderingPolicy {

  final static int GB = 1024;

  @Test
  public void testCriticalJobsWithSamePriorityIterators() {
    CompositeWeightOrderingPolicy<MockSchedulableEntity> schedOrder =
        new CompositeWeightOrderingPolicy<MockSchedulableEntity>();

    long cacheTime = 3000;
    double highFlagPriority = 60;
    double pendingFlagMemory = 100 * 1024 * 1024;
    double pendingFlagTime = 120 * 60 * 1000;
    double priorityWeight = 0.6;
    double pendingResourcesWeight = 0.2;
    double pendingTimeWeight = 0.2;
    schedOrder.setCacheTime(cacheTime);
    schedOrder.setHighFlagPriority(highFlagPriority);
    schedOrder.setPendingFlagMemory(pendingFlagMemory);
    schedOrder.setPendingFlagTime(pendingFlagTime);
    schedOrder.setPriorityWeightFactor(priorityWeight);
    schedOrder.setPendingMemoryWeightFactor(pendingResourcesWeight);
    schedOrder.setPendingTimeWeightFactor(pendingTimeWeight);

    MockSchedulableEntity r1 = new MockSchedulableEntity();
    MockSchedulableEntity r2 = new MockSchedulableEntity();
    r1.setId("1");
    r2.setId("2");

    //Set priority
    Priority p1 = Priority.newInstance(70);
    Priority p2 = Priority.newInstance(70);
    r1.setApplicationPriority(p1);
    r2.setApplicationPriority(p2);

    //Set start time, r1 pending 10 minutes, r2 pending 20minutes
    long currentTime = System.currentTimeMillis();
    r1.setStartTime(currentTime - 10 * 60 * 1000);
    r2.setStartTime(currentTime - 20 * 60 * 1000);

    schedOrder.addSchedulableEntity(r1);
    schedOrder.addSchedulableEntity(r2);

    //Assignment, old jobs to new jobs
    checkIds(schedOrder.getAssignmentIterator(
        IteratorSelector.EMPTY_ITERATOR_SELECTOR),
        new String[]{"2","1"});
  }

  @Test
  public void testCriticalJobsWithDifferentPriorityIterators() {
    CompositeWeightOrderingPolicy<MockSchedulableEntity> schedOrder =
        new CompositeWeightOrderingPolicy<MockSchedulableEntity>();

    long cacheTime = 3000;
    double highFlagPriority = 60;
    double pendingFlagMemory = 100 * 1024 * 1024;
    double pendingFlagTime = 120 * 60 * 1000;
    double priorityWeight = 0.6;
    double pendingResourcesWeight = 0.2;
    double pendingTimeWeight = 0.2;
    schedOrder.setCacheTime(cacheTime);
    schedOrder.setHighFlagPriority(highFlagPriority);
    schedOrder.setPendingFlagMemory(pendingFlagMemory);
    schedOrder.setPendingFlagTime(pendingFlagTime);
    schedOrder.setPriorityWeightFactor(priorityWeight);
    schedOrder.setPendingMemoryWeightFactor(pendingResourcesWeight);
    schedOrder.setPendingTimeWeightFactor(pendingTimeWeight);

    MockSchedulableEntity r1 = new MockSchedulableEntity();
    MockSchedulableEntity r2 = new MockSchedulableEntity();
    r1.setId("1");
    r2.setId("2");

    //Set priority
    Priority p1 = Priority.newInstance(70);
    Priority p2 = Priority.newInstance(65);
    r1.setApplicationPriority(p1);
    r2.setApplicationPriority(p2);

    //Set start time, r1 pending 10 minutes, r2 pending 200minutes
    long currentTime = System.currentTimeMillis();
    r1.setStartTime(currentTime - 10 * 60 * 1000);
    r2.setStartTime(currentTime - 200 * 60 * 1000);

    //Set pending resources, r1 pending 10TB, r2 pending 200TB
    r1.setPending(Resources.createResource(10 * 1024 * GB));
    r2.setPending(Resources.createResource(200 * 1024 * GB));

    AbstractComparatorOrderingPolicy
        .updateSchedulingResourceUsage(r1.getSchedulingResourceUsage());
    AbstractComparatorOrderingPolicy
        .updateSchedulingResourceUsage(r2.getSchedulingResourceUsage());

    schedOrder.addSchedulableEntity(r1);
    schedOrder.addSchedulableEntity(r2);

    //Assignment, high priority jobs to low priority jobs
    checkIds(schedOrder.getAssignmentIterator(
        IteratorSelector.EMPTY_ITERATOR_SELECTOR),
        new String[]{"1","2"});
  }

  @Test
  public void testCriticalAndNonCriticalJobsIterators() {
    CompositeWeightOrderingPolicy<MockSchedulableEntity> schedOrder =
        new CompositeWeightOrderingPolicy<MockSchedulableEntity>();

    long cacheTime = 3000;
    double highFlagPriority = 60;
    double pendingFlagMemory = 100 * 1024 * 1024;
    double pendingFlagTime = 120 * 60 * 1000;
    double priorityWeight = 0.6;
    double pendingResourcesWeight = 0.2;
    double pendingTimeWeight = 0.2;
    schedOrder.setCacheTime(cacheTime);
    schedOrder.setHighFlagPriority(highFlagPriority);
    schedOrder.setPendingFlagMemory(pendingFlagMemory);
    schedOrder.setPendingFlagTime(pendingFlagTime);
    schedOrder.setPriorityWeightFactor(priorityWeight);
    schedOrder.setPendingMemoryWeightFactor(pendingResourcesWeight);
    schedOrder.setPendingTimeWeightFactor(pendingTimeWeight);

    MockSchedulableEntity r1 = new MockSchedulableEntity();
    MockSchedulableEntity r2 = new MockSchedulableEntity();
    r1.setId("1");
    r2.setId("2");

    //Set priority
    Priority p1 = Priority.newInstance(70);
    Priority p2 = Priority.newInstance(40);
    r1.setApplicationPriority(p1);
    r2.setApplicationPriority(p2);

    //Set start time, r1 pending 10 minutes, r2 pending 200minutes
    long currentTime = System.currentTimeMillis();
    r1.setStartTime(currentTime - 10 * 60 * 1000);
    r2.setStartTime(currentTime - 200 * 60 * 1000);

    //Set pending resources, r1 pending 10TB, r2 pending 200TB
    r1.setPending(Resources.createResource(10 * 1024 * GB));
    r2.setPending(Resources.createResource(200 * 1024 * GB));

    AbstractComparatorOrderingPolicy
        .updateSchedulingResourceUsage(r1.getSchedulingResourceUsage());
    AbstractComparatorOrderingPolicy
        .updateSchedulingResourceUsage(r2.getSchedulingResourceUsage());

    schedOrder.addSchedulableEntity(r1);
    schedOrder.addSchedulableEntity(r2);

    //Assignment, high priority jobs to low priority jobs
    checkIds(schedOrder.getAssignmentIterator(
        IteratorSelector.EMPTY_ITERATOR_SELECTOR),
        new String[]{"1","2"});
  }

  @Test
  public void testAllJobsIterators() {

    //System configs like below:
    /**
     * highFlagPriority = 60
     * pendingFlagMemory = 100 * 1024 * 1024;
     * pendingFlagTime = 120 * 60 * 1000;
     * priorityWeight = 0.6
     * pendingResourcesWeight = 0.2
     * pendingTimeWeight = 0.2
     */

    //we start 4 example apps, like below:
    /**
     *
     * app1: priority=10, pending 60TB resources, started 120 minutes
     * app2: priority=30, pending 100TB resources, started 80 minutes
     * app3: priority=50, pending 20TB resources, started 20 minutes
     * app4: priority=60, pending 20TB resources, started 15 minutes
     *
     * app1_Weighted_Priority = (10 / 60) * 0.6 + (60 / 100) * 0.2 + (120 / 120) * 0.2 = 0.42
     * app2_Weighted_Priority = (30 / 60) * 0.6 + (100 / 100) * 0.2 + (80 / 120) * 0.2 = 0.633
     * app3_Weighted_Priority = (50 / 60) * 0.6 + (20 / 100) * 0.2 + (20 / 120) * 0.2 = 0.573
     * app4 is critical job, we only compare priority with other jobs, so it has biggest priority
     */

    CompositeWeightOrderingPolicy<MockSchedulableEntity> schedOrder =
     new CompositeWeightOrderingPolicy<MockSchedulableEntity>();

    long cacheTime = 3000;
    double highFlagPriority = 60;
    double pendingFlagMemory = 100 * 1024 * 1024;
    double pendingFlagTime = 120 * 60 * 1000;
    double priorityWeight = 0.6;
    double pendingResourcesWeight = 0.2;
    double pendingTimeWeight = 0.2;
    schedOrder.setCacheTime(cacheTime);
    schedOrder.setHighFlagPriority(highFlagPriority);
    schedOrder.setPendingFlagMemory(pendingFlagMemory);
    schedOrder.setPendingFlagTime(pendingFlagTime);
    schedOrder.setPriorityWeightFactor(priorityWeight);
    schedOrder.setPendingMemoryWeightFactor(pendingResourcesWeight);
    schedOrder.setPendingTimeWeightFactor(pendingTimeWeight);

    MockSchedulableEntity r1 = new MockSchedulableEntity();
    MockSchedulableEntity r2 = new MockSchedulableEntity();
    MockSchedulableEntity r3 = new MockSchedulableEntity();
    MockSchedulableEntity r4 = new MockSchedulableEntity();

    r1.setId("1");
    r2.setId("2");
    r3.setId("3");
    r4.setId("4");

    //Set priority
    Priority p1 = Priority.newInstance(10);
    Priority p2 = Priority.newInstance(30);
    Priority p3 = Priority.newInstance(50);
    Priority p4 = Priority.newInstance(60);
    r1.setApplicationPriority(p1);
    r2.setApplicationPriority(p2);
    r3.setApplicationPriority(p3);
    r4.setApplicationPriority(p4);

    //Set pending resources
    r1.setPending(Resources.createResource(60 * 1024 * GB));
    r2.setPending(Resources.createResource(100 * 1024 * GB));
    r3.setPending(Resources.createResource(20 * 1024 * GB));
    r4.setPending(Resources.createResource(20 * 1024 * GB));
    AbstractComparatorOrderingPolicy
        .updateSchedulingResourceUsage(r1.getSchedulingResourceUsage());
    AbstractComparatorOrderingPolicy
        .updateSchedulingResourceUsage(r2.getSchedulingResourceUsage());
    AbstractComparatorOrderingPolicy
        .updateSchedulingResourceUsage(r3.getSchedulingResourceUsage());
    AbstractComparatorOrderingPolicy
        .updateSchedulingResourceUsage(r4.getSchedulingResourceUsage());

    //Set pending time
    long currentTime = System.currentTimeMillis();
    r1.setStartTime(currentTime - 120 * 60 * 1000);
    r2.setStartTime(currentTime - 80 * 60 * 1000);
    r3.setStartTime(currentTime - 20 * 60 * 1000);
    r4.setStartTime(currentTime - 15 * 60 * 1000);

    schedOrder.addSchedulableEntity(r1);
    schedOrder.addSchedulableEntity(r2);
    schedOrder.addSchedulableEntity(r3);
    schedOrder.addSchedulableEntity(r4);

    //Assignment, greatest to least weight
    checkIds(schedOrder.getAssignmentIterator(
        IteratorSelector.EMPTY_ITERATOR_SELECTOR),
        new String[]{"4","2", "3", "1"});
  }

  @Test
  public void testAllJobsIteratorsWithCache() throws InterruptedException {

    CompositeWeightOrderingPolicy<MockSchedulableEntity> schedOrder =
        new CompositeWeightOrderingPolicy<MockSchedulableEntity>();

    long cacheTime = 3000;
    double highFlagPriority = 60;
    double pendingFlagMemory = 100 * 1024 * 1024;
    double pendingFlagTime = 120 * 60 * 1000;
    double priorityWeight = 0.6;
    double pendingResourcesWeight = 0.2;
    double pendingTimeWeight = 0.2;
    schedOrder.setCacheTime(cacheTime);
    schedOrder.setHighFlagPriority(highFlagPriority);
    schedOrder.setPendingFlagMemory(pendingFlagMemory);
    schedOrder.setPendingFlagTime(pendingFlagTime);
    schedOrder.setPriorityWeightFactor(priorityWeight);
    schedOrder.setPendingMemoryWeightFactor(pendingResourcesWeight);
    schedOrder.setPendingTimeWeightFactor(pendingTimeWeight);

    MockSchedulableEntity r1 = new MockSchedulableEntity();
    MockSchedulableEntity r2 = new MockSchedulableEntity();
    MockSchedulableEntity r3 = new MockSchedulableEntity();
    MockSchedulableEntity r4 = new MockSchedulableEntity();

    r1.setId("1");
    r2.setId("2");
    r3.setId("3");
    r4.setId("4");

    //Set priority
    Priority p1 = Priority.newInstance(10);
    Priority p2 = Priority.newInstance(30);
    Priority p3 = Priority.newInstance(50);
    Priority p4 = Priority.newInstance(60);
    r1.setApplicationPriority(p1);
    r2.setApplicationPriority(p2);
    r3.setApplicationPriority(p3);
    r4.setApplicationPriority(p4);

    //Set pending resources
    r1.setPending(Resources.createResource(60 * 1024 * GB));
    r2.setPending(Resources.createResource(100 * 1024 * GB));
    r3.setPending(Resources.createResource(20 * 1024 * GB));
    r4.setPending(Resources.createResource(20 * 1024 * GB));
    AbstractComparatorOrderingPolicy
        .updateSchedulingResourceUsage(r1.getSchedulingResourceUsage());
    AbstractComparatorOrderingPolicy
        .updateSchedulingResourceUsage(r2.getSchedulingResourceUsage());
    AbstractComparatorOrderingPolicy
        .updateSchedulingResourceUsage(r3.getSchedulingResourceUsage());
    AbstractComparatorOrderingPolicy
        .updateSchedulingResourceUsage(r4.getSchedulingResourceUsage());

    //Set pending time
    long currentTime = System.currentTimeMillis();
    r1.setStartTime(currentTime - 120 * 60 * 1000);
    r2.setStartTime(currentTime - 80 * 60 * 1000);
    r3.setStartTime(currentTime - 20 * 60 * 1000);
    r4.setStartTime(currentTime - 15 * 60 * 1000);

    schedOrder.addSchedulableEntity(r1);
    schedOrder.addSchedulableEntity(r2);
    schedOrder.addSchedulableEntity(r3);
    schedOrder.addSchedulableEntity(r4);

    //Assignment, greatest to least weight
    long startTime1 = System.nanoTime();
    Iterator<MockSchedulableEntity> iterator1 = schedOrder.getAssignmentIterator(
        IteratorSelector.EMPTY_ITERATOR_SELECTOR);
    long endTime1 = System.nanoTime();
    long costTime1 = endTime1 - startTime1;
    checkIds(iterator1, new String[]{"4","2", "3", "1"});

    //Change value with cache, should see no change for assignmentIterator
    r3.setPending(Resources.createResource(100 * 1024 * GB));
    schedOrder.containerAllocated(r3, null);
    long startTime2 = System.nanoTime();
    Iterator<MockSchedulableEntity> iterator2 = schedOrder.getAssignmentIterator(
        IteratorSelector.EMPTY_ITERATOR_SELECTOR);
    long endTime2 = System.nanoTime();
    long costTime2 = endTime2 - startTime2;
    checkIds(iterator2, new String[]{"4", "2", "3", "1"});

    //Cache time out, will reorder
    Thread.sleep(2 * cacheTime);
    /**
     *
     * app1: priority=10, pending 60TB resources, started 120 minutes
     * app2: priority=30, pending 100TB resources, started 80 minutes
     * app3: priority=50, pending 100TB resources, started 20 minutes
     * app4: priority=60, pending 20TB resources, started 15 minutes
     *
     * app1_Weighted_Priority = (10 / 60) * 0.6 + (60 / 100) * 0.2 + (120 / 120) * 0.2 = 0.42
     * app2_Weighted_Priority = (30 / 60) * 0.6 + (100 / 100) * 0.2 + (80 / 120) * 0.2 = 0.633
     * app3_Weighted_Priority = (50 / 60) * 0.6 + (100 / 100) * 0.2 + (20 / 120) * 0.2 = 0.733
     * app4 is critical job, we only compare priority with other jobs, so it has biggest priority
     */
    long startTime3 = System.nanoTime();
    Iterator<MockSchedulableEntity> iterator3 = schedOrder.getAssignmentIterator(
        IteratorSelector.EMPTY_ITERATOR_SELECTOR);
    long endTime3 = System.nanoTime();
    long costTime3 = endTime3 - startTime3;
    checkIds(iterator3, new String[]{"4", "3", "2", "1"});

    assertTrue(costTime2 < costTime1);
    assertTrue(costTime2 < costTime3);

  }

  public void checkIds(Iterator<MockSchedulableEntity> si,
      String[] ids) {
    for (int i = 0;i < ids.length;i++) {
      Assert.assertEquals(si.next().getId(),
        ids[i]);
    }
  }

}
