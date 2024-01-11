package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestCapacitySchedulerAppSelector
    extends CapacitySchedulerTestBase {
  private MockRM mockRM = null;

  private static void setAppSelector(CapacitySchedulerConfiguration conf) {
    conf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] { "a", "b" });

    conf.setCapacity(A, A_CAPACITY);
    conf.setCapacity(B, B_CAPACITY);

    // Define 2nd-level queues
    conf.setQueues(A, new String[] { "a1", "a2" });
    conf.setCapacity(A1, A1_CAPACITY);
    conf.setCapacity(A2, A2_CAPACITY);

    conf.setQueues(B, new String[] { "b1", "b2", "b3" });
    conf.setCapacity(B1, B1_CAPACITY);
    conf.setCapacity(B2, B2_CAPACITY);
    conf.setCapacity(B3, B3_CAPACITY);

    conf.set("yarn.scheduler.capacity.app-selectors", "colocation,night-shift");
    conf.set("yarn.scheduler.capacity.app-selectors.colocation.filters",
        "env,low_priority");
    conf.set(
        "yarn.scheduler.capacity.app-selectors.colocation.filters.env.params",
        "rssEnabled=true");
    conf.set(
        "yarn.scheduler.capacity.app-selectors.colocation.filters.low_priority.params",
        "priorityThreshold=40");
  }

  @Test
  public void testAppSelector() {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    setAppSelector(conf);
    mockRM = new MockRM(conf);
    CapacityScheduler cs = (CapacityScheduler) mockRM.getResourceScheduler();
    mockRM.start();
    cs.start();
    assertNotNull(
        cs.getAppSelectorManager().getMappedAppSelector("colocation"));
    assertNotNull(cs.getAppSelectorManager().getMappedAppSelector("colocation")
        .getFilterMap().get("env"));
    assertNotNull(cs.getAppSelectorManager().getMappedAppSelector("colocation")
        .getFilterMap().get("low_priority"));

    assertNotNull(
        cs.getAppSelectorManager().getMappedAppSelector("night-shift"));
    assertEquals(0,
        cs.getAppSelectorManager().getMappedAppSelector("night-shift")
            .getFilterMap().size());
  }

  @Test
  public void testSchedulableEntityFilter() {
    Map<String, String> priorityConfig = new HashMap<>();
    priorityConfig.put("priorityThreshold", "40");
    SchedulableEntityFilter priorityFilter =
        AppSelectorFilterUtils.getFilter("low_priority", priorityConfig);
    MockSchedulableEntity entity = new MockSchedulableEntity(1, 40, false);
    assertEquals(true, priorityFilter.filter(entity));

    Map<String, String> envConfig = new HashMap<>();
    envConfig.put("rssEnabled", "true");
    SchedulableEntityFilter envFilter =
        AppSelectorFilterUtils.getFilter("env", envConfig);
    MockSchedulableEntity entity2 = new MockSchedulableEntity(2, 40, false);
    Map<String, String> appEnvs = new HashMap<>();
    appEnvs.put("rssEnabled", "true");
    ContainerLaunchContext clc = ContainerLaunchContext
        .newInstance(null, appEnvs, null, null, null, null);
    ApplicationSubmissionContext asc = ApplicationSubmissionContext
        .newInstance(null, null, null, null, clc, false, false, 1, null);
    entity2.setAppSubmissionContext(asc);
    assertEquals(true, envFilter.filter(entity2));
  }

  @Test
  public void testIterators() {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    setAppSelector(conf);
    mockRM = new MockRM(conf);
    CapacityScheduler cs = (CapacityScheduler) mockRM.getResourceScheduler();
    mockRM.start();
    cs.start();

    OrderingPolicy<MockSchedulableEntity> schedOrder =
        new FairOrderingPolicy<>();
    MockSchedulableEntity msp1 = new MockSchedulableEntity(1, 20, false);
    MockSchedulableEntity msp2 = new MockSchedulableEntity(2, 30, false);
    MockSchedulableEntity msp3 = new MockSchedulableEntity(3, 80, false);
    MockSchedulableEntity msp4 = new MockSchedulableEntity(4, 40, false);

    msp1.setId("1");
    msp1.setAppSubmissionContext(
        constructApplicationSubmissionContext(new HashMap<String, String>() {{
        }}));
    msp2.setId("2");
    msp2.setAppSubmissionContext(
        constructApplicationSubmissionContext(new HashMap<String, String>() {{
          put("rssEnabled", "true");
        }}));
    msp3.setId("3");
    msp4.setId("4");
    msp4.setAppSubmissionContext(
        constructApplicationSubmissionContext(new HashMap<String, String>() {{
          put("rssEnabled", "true");
        }}));

    schedOrder.addSchedulableEntity(msp1);
    schedOrder.addSchedulableEntity(msp2);
    schedOrder.addSchedulableEntity(msp3);
    schedOrder.addSchedulableEntity(msp4);

    IteratorSelector iteratorSelector = new IteratorSelector();
    iteratorSelector.setAppSelector(
        cs.getAppSelectorManager().getMappedAppSelector("colocation"));
    Iterator<MockSchedulableEntity> iter =
        schedOrder.getAssignmentIterator(iteratorSelector);

    assertEquals("2", iter.next().getId());
    assertEquals("4", iter.next().getId());

    Iterator<MockSchedulableEntity> iter2 = schedOrder
        .getAssignmentIterator(IteratorSelector.EMPTY_ITERATOR_SELECTOR);
    String[] ids = new String[] { "1", "2", "3" };
    for (int i = 0; i < ids.length; i++) {
      assertEquals(iter2.next().getId(), ids[i]);
    }
  }

  public ApplicationSubmissionContext constructApplicationSubmissionContext(
      Map<String, String> appEnvs) {
    ContainerLaunchContext clc = ContainerLaunchContext
        .newInstance(null, appEnvs, null, null, null, null);
    ApplicationSubmissionContext asc = ApplicationSubmissionContext
        .newInstance(null, null, null, null, clc, false, false, 1, null);
    return asc;
  }
}
