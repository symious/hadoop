package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.policy;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueResourceQuotas;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacities;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestUtilization2RandomQueueOrderingPolicy {

  final static int GB = 1024;

  private List<CSQueue> mockCSQueues(String[] queueNames, float[] utilizations,
      String partition) {
    // sanity check
    assert queueNames != null && utilizations != null
        && queueNames.length > 0 && queueNames.length == utilizations.length;

    List<CSQueue> list = new ArrayList<>();
    for (int i = 0; i < queueNames.length; i++) {
      CSQueue q = mock(CSQueue.class);
      when(q.getQueuePath()).thenReturn(queueNames[i]);

      QueueCapacities qc = new QueueCapacities(false);
      qc.setUsedCapacity(partition, utilizations[i]);
      when(q.getQueueCapacities()).thenReturn(qc);

      QueueResourceQuotas qr = new QueueResourceQuotas();
      when(q.getQueueResourceQuotas()).thenReturn(qr);
      list.add(q);

      //add queue pending resources
      doReturn(mock(QueueMetrics.class)).when(q).getMetrics();
      when(q.getMetrics().getPendingMB()).thenReturn(1L * GB);
      when(q.getMetrics().getPendingVirtualCores()).thenReturn(1);
    }

    return list;
  }

  private void verifyOrder(QueueOrderingPolicy orderingPolicy, String partition,
      String[] expectedOrder) {
    Iterator<CSQueue> iter = orderingPolicy.getAssignmentIterator(partition);
    int i = 0;
    while (iter.hasNext()) {
      CSQueue q = iter.next();
      Assert.assertEquals(expectedOrder[i], q.getQueuePath());
      i++;
    }

    assert i == expectedOrder.length;
  }

  private String getFirstOne(QueueOrderingPolicy orderingPolicy, String partition) {
    Iterator<CSQueue> iter = orderingPolicy.getAssignmentIterator(partition);
    if (iter.hasNext()) {
      CSQueue q = iter.next();
      return q.getQueuePath();
    }
    return "";
  }

  @Test
  public void testUtilization2RandomOrdering() throws InterruptedException {

    long cacheTime = 3000;

    Utilization2RandomQueueOrderingPolicy policy =
        new Utilization2RandomQueueOrderingPolicy(true,
            1, cacheTime);

    // Case 1, 2 queues, without cache
    policy.setQueues(
        mockCSQueues(new String[] {"a", "b"}, new float[] {1.1f, 0.0f}, ""));
    verifyOrder(policy, "", new String[] {"b", "a"});

    // Case 2, 3 queues, with cache, though a is lowest utilization
    // will first choose b
    policy.setQueues(
        mockCSQueues(new String[] {"a", "b", "c"},
            new float[] {0.5f, 1.1f, 1.5f}, ""));
    Assert.assertEquals("b", getFirstOne(policy, ""));

    // Case 3, 3 queues same with case2, without cache, will first choose a
    Thread.sleep(cacheTime);
    policy.setQueues(
        mockCSQueues(new String[] {"a", "b", "c"},
            new float[] {0.5f, 1.1f, 1.5f}, ""));
    Assert.assertEquals("a", getFirstOne(policy, ""));

    // Case 4, 3 queues, with different accessibility to partition
    // only a can access "x"
    // without cache, will first choose a
    List<CSQueue>
        queues = mockCSQueues(new String[] {"a", "b", "c"},
        new float[] {0.1f, 0.0f, 0.2f}, "x");
    when(queues.get(0).getAccessibleNodeLabels()).thenReturn(
        ImmutableSet.of("x", "y"));
    Thread.sleep(cacheTime);
    policy.setQueues(queues);
    Assert.assertEquals("a", getFirstOne(policy, "x"));

  }
}
