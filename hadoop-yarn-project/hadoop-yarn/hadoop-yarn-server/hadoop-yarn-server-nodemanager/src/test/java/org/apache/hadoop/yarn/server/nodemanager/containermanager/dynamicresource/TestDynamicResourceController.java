package org.apache.hadoop.yarn.server.nodemanager.containermanager.dynamicresource;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestDynamicResourceController {


  @Test
  public void testMergeIncreaseAndDecrease() {
    DynamicResourceController controller = new DynamicResourceController(new YarnConfiguration(), null);

    List<ContainerAdjustment> adjustments= new ArrayList<>();
    controller.mergeIncreaseAndDecrease(adjustments);
    Assert.assertEquals(0, adjustments.size());

    /*
       Before merge: C1<INCREASE 1024Mb> C2<INCREASE 1024Mb>
       After merge:  C1<INCREASE 1024Mb> C2<INCREASE 1024Mb>
     */
    adjustments.add(new ContainerAdjustment(ContainerId.newContainerId(
        ApplicationAttemptId.fromString("appattempt_1689585553491_1925032_000001"), 1),
        null, ContainerUpdateType.INCREASE_RESOURCE, null, 1024));
    adjustments.add(new ContainerAdjustment(ContainerId.newContainerId(
        ApplicationAttemptId.fromString("appattempt_1689585553491_1925032_000001"), 2),
        null, ContainerUpdateType.INCREASE_RESOURCE, null, 1024));
    controller.mergeIncreaseAndDecrease(adjustments);
    Assert.assertEquals(2, adjustments.size());
    for (ContainerAdjustment ca : adjustments) {
      Assert.assertEquals(1024, ca.deltaMemory);
    }

    /*
       Before merge: C1<DECREASE -1024Mb> C2<DECREASE -1024Mb>
       After merge:  C1<DECREASE -1024Mb> C2<DECREASE -1024Mb>
     */
    adjustments.clear();
    adjustments.add(new ContainerAdjustment(ContainerId.newContainerId(
        ApplicationAttemptId.fromString("appattempt_1689585553491_1925032_000001"), 1),
        null, ContainerUpdateType.DECREASE_RESOURCE, null, -1024));
    adjustments.add(new ContainerAdjustment(ContainerId.newContainerId(
        ApplicationAttemptId.fromString("appattempt_1689585553491_1925032_000001"), 2),
        null, ContainerUpdateType.DECREASE_RESOURCE, null, -1024));
    controller.mergeIncreaseAndDecrease(adjustments);
    Assert.assertEquals(2, adjustments.size());
    for (ContainerAdjustment ca : adjustments) {
      Assert.assertEquals(-1024, ca.deltaMemory);
    }

    /*
       Before merge: C1<DECREASE -2048Mb> C2<DECREASE -2048Mb> C3<INCREASE 2048Mb> C4<INCREASE 4096Mb>
       After merge:  C3<INCREASE 2048Mb> C4<INCREASE 4096Mb>
     */
    adjustments.clear();
    adjustments.add(new ContainerAdjustment(ContainerId.newContainerId(
        ApplicationAttemptId.fromString("appattempt_1689585553491_1925032_000001"), 1),
        null, ContainerUpdateType.DECREASE_RESOURCE, null, -2048));
    adjustments.add(new ContainerAdjustment(ContainerId.newContainerId(
        ApplicationAttemptId.fromString("appattempt_1689585553491_1925032_000001"), 2),
        null, ContainerUpdateType.DECREASE_RESOURCE, null, -2048));
    adjustments.add(new ContainerAdjustment(ContainerId.newContainerId(
        ApplicationAttemptId.fromString("appattempt_1689585553491_1925032_000001"), 3),
        null, ContainerUpdateType.INCREASE_RESOURCE, null, 2048));
    adjustments.add(new ContainerAdjustment(ContainerId.newContainerId(
        ApplicationAttemptId.fromString("appattempt_1689585553491_1925032_000001"), 4),
        null, ContainerUpdateType.INCREASE_RESOURCE, null, 4096));
    controller.mergeIncreaseAndDecrease(adjustments);
    Assert.assertEquals(2, adjustments.size());
    for (ContainerAdjustment ca : adjustments) {
      if (ca.getContainerId().getId() == 3) {
        Assert.assertEquals(2048, ca.deltaMemory);
      } else if (ca.getContainerId().getId() == 4) {
        Assert.assertEquals(4096, ca.deltaMemory);
      }
    }

    /*
       Before merge: C1<DECREASE -2048Mb> C2<DECREASE -2048Mb> C3<INCREASE 2048Mb>
       After merge:  C1<DECREASE -1024Mb> C2<DECREASE -1024Mb> C3<INCREASE 2048Mb>
     */
    adjustments.clear();
    adjustments.add(new ContainerAdjustment(ContainerId.newContainerId(
        ApplicationAttemptId.fromString("appattempt_1689585553491_1925032_000001"), 1),
        null, ContainerUpdateType.DECREASE_RESOURCE, null, -2048));
    adjustments.add(new ContainerAdjustment(ContainerId.newContainerId(
        ApplicationAttemptId.fromString("appattempt_1689585553491_1925032_000001"), 2),
        null, ContainerUpdateType.DECREASE_RESOURCE, null, -2048));
    adjustments.add(new ContainerAdjustment(ContainerId.newContainerId(
        ApplicationAttemptId.fromString("appattempt_1689585553491_1925032_000001"), 3),
        null, ContainerUpdateType.INCREASE_RESOURCE, null, 2048));
    controller.mergeIncreaseAndDecrease(adjustments);
    Assert.assertEquals(3, adjustments.size());
    for (ContainerAdjustment ca : adjustments) {
      if (ca.getContainerUpdateType().equals(ContainerUpdateType.INCREASE_RESOURCE)) {
        Assert.assertEquals(2048, ca.deltaMemory);
      } else {
        Assert.assertEquals(-1024, ca.deltaMemory);
      }
    }

    /*
       Before merge: C1<DECREASE -2048Mb> C2<DECREASE -2048Mb> C3<DECREASE -3072Mb> C4<INCREASE 2048Mb> C5<INCREASE 2048Mb>
       After merge:  C2<DECREASE -1024Mb> C3<DECREASE -2048Mb> C4<INCREASE 2048Mb> C5<INCREASE 2048Mb>
     */
    adjustments.clear();
    adjustments.add(new ContainerAdjustment(ContainerId.newContainerId(
        ApplicationAttemptId.fromString("appattempt_1689585553491_1925032_000001"), 1),
        null, ContainerUpdateType.DECREASE_RESOURCE, null, -2048));
    adjustments.add(new ContainerAdjustment(ContainerId.newContainerId(
        ApplicationAttemptId.fromString("appattempt_1689585553491_1925032_000001"), 2),
        null, ContainerUpdateType.DECREASE_RESOURCE, null, -2048));
    adjustments.add(new ContainerAdjustment(ContainerId.newContainerId(
        ApplicationAttemptId.fromString("appattempt_1689585553491_1925032_000001"), 3),
        null, ContainerUpdateType.DECREASE_RESOURCE, null, -3072));
    adjustments.add(new ContainerAdjustment(ContainerId.newContainerId(
        ApplicationAttemptId.fromString("appattempt_1689585553491_1925032_000001"), 4),
        null, ContainerUpdateType.INCREASE_RESOURCE, null, 2048));
    adjustments.add(new ContainerAdjustment(ContainerId.newContainerId(
        ApplicationAttemptId.fromString("appattempt_1689585553491_1925032_000001"), 5),
        null, ContainerUpdateType.INCREASE_RESOURCE, null, 2048));
    controller.mergeIncreaseAndDecrease(adjustments);
    Assert.assertEquals(4, adjustments.size());
    Assert.assertEquals(-1024, adjustments.get(0).deltaMemory);
    Assert.assertEquals(-2048, adjustments.get(1).deltaMemory);
  }
}
