package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestDynamicAdjustment {

  private final int GB = 1024;

  private YarnConfiguration conf;

  RMNodeLabelsManager mgr;

  @Before
  public void setUp() throws Exception {
    CapacitySchedulerConfiguration csConf
        = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(csConf);
    setDynamicAdjustmentSwitch(csConf);

    conf = new YarnConfiguration(csConf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);
  }

  @Test
  public void testDynamicAdjustmentConf() {
    MockRM rm = new MockRM(conf);
    rm.init(conf);

    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    System.out.println(cs.getRootQueue().getChildQueues());
    assertEquals(false, cs.getRootQueue().getFeatureEnabled("dynamic-adjustment"));
    assertEquals(true, cs.getQueue("a").getFeatureEnabled("dynamic-adjustment"));
  }

  private CapacitySchedulerConfiguration setupQueueConfiguration(
      CapacitySchedulerConfiguration conf) {
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {"a", "b"});

    conf.setCapacity("root.a", 89.5f);
    conf.setCapacity("root.b", 10.5f);

    return conf;
  }

  private void setDynamicAdjustmentSwitch(CapacitySchedulerConfiguration conf) {
    conf.set("yarn.scheduler.capacity.root.dynamic-adjustment.enabled", "false");
    conf.set("yarn.scheduler.capacity.root.a.dynamic-adjustment.enabled", "true");
  }

  @Test
  public void testA() throws Exception {
    MockRM rm1 = new MockRM(conf);
    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 10 * GB);
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("a")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    am1.allocate("*", 1 * GB, 2, new ArrayList<ContainerId>());

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();

    CapacitySchedulerConfiguration newCSConf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(newCSConf);
    newCSConf.setInt(
        CapacitySchedulerConfiguration.OFFSWITCH_PER_HEARTBEAT_LIMIT, 1);
    setDynamicAdjustmentSwitch(newCSConf);
    cs.reinitialize(newCSConf, rm1.getRMContext());

    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    Assert.assertEquals(1, cs.getNode(nm1.getNodeId()).getNumContainers());
    Assert.assertEquals(1024, cs.getNode(nm1.getNodeId()).getAllocatedResource().getMemorySize());
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    Assert.assertEquals(2, cs.getNode(nm1.getNodeId()).getNumContainers());
    Assert.assertEquals(2048, cs.getNode(nm1.getNodeId()).getAllocatedResource().getMemorySize());
    Assert.assertEquals(2, cs.getNode(nm1.getNodeId()).getCopiedListOfRunningContainers().size());
    Assert.assertEquals(2, cs.getApplicationAttempt(am1.getApplicationAttemptId()).getLiveContainersMap().keySet().size());

    ContainerId containerId2 =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
    rm1.getResourceScheduler().getRMContainer(containerId2).getContainer().getVersion();

    List<UpdateContainerRequest> update = new ArrayList<>();
    update.add(UpdateContainerRequest.newInstance(
        rm1.getResourceScheduler().getRMContainer(containerId2).getContainer()
            .getVersion(), containerId2, ContainerUpdateType.INCREASE_RESOURCE,
        Resource.newInstance(3072, 1), ExecutionType.GUARANTEED));
    AllocateRequest allocateRequest = AllocateRequest.newInstance(0, 0, new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>(), update, null);

    am1.allocate(allocateRequest);
    am1.allocate(allocateRequest);
    newCSConf.setInt(
        CapacitySchedulerConfiguration.OFFSWITCH_PER_HEARTBEAT_LIMIT, 2);
    cs.reinitialize(newCSConf, rm1.getRMContext());
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));

    Assert.assertEquals(5120, cs.getNode(nm1.getNodeId()).getAllocatedResource().getMemorySize());
    Assert.assertEquals(3, cs.getNode(nm1.getNodeId()).getAllocatedResource().getVirtualCores());
    Assert.assertEquals(5120, cs.getNode(nm1.getNodeId()).getUnallocatedResource().getMemorySize());

    update.clear();
    allocateRequest.setUpdateRequests(update);
    am1.allocate(allocateRequest);

    update.add(UpdateContainerRequest.newInstance(
        rm1.getResourceScheduler().getRMContainer(containerId2).getContainer()
            .getVersion(), containerId2, ContainerUpdateType.DECREASE_RESOURCE,
        Resource.newInstance(2048, 1), ExecutionType.GUARANTEED));
    allocateRequest.setUpdateRequests(update);
    am1.allocate(allocateRequest);

    Thread.sleep(2000);
    Assert.assertEquals(6144, cs.getQueue("root").getMetrics().getAvailableMB());
    Assert.assertEquals(4096, cs.getQueue("root").getMetrics().getAllocatedMB());
    Assert.assertEquals(3, cs.getQueue("root").getMetrics().getAllocatedContainers());
  }
}
