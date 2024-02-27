package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.policy;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacities;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestUtilization2RandomQueueOrderingPolicy {

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

      list.add(q);

      ResourceUsage resUsagePerQueue = new ResourceUsage();
      when(q.getQueueResourceUsage()).thenReturn(resUsagePerQueue);
    }

    return list;
  }

  private void verifyOrder(QueueOrderingPolicy orderingPolicy, String partition,
      Set<String> otherLookupPartitions, String[] expectedOrder) {
    Iterator<CSQueue> iter =
        orderingPolicy.getAssignmentIterator(partition, otherLookupPartitions);
    int i = 0;
    while (iter.hasNext()) {
      CSQueue q = iter.next();
      Assert.assertEquals(expectedOrder[i], q.getQueuePath());
      i++;
    }

    assert i == expectedOrder.length;
  }

  private String getFirstOne(QueueOrderingPolicy orderingPolicy,
      String partition, Set<String> otherLookupPartitions) {
    Iterator<CSQueue> iter =
        orderingPolicy.getAssignmentIterator(partition, otherLookupPartitions);
    if (iter.hasNext()) {
      CSQueue q = iter.next();
      return q.getQueuePath();
    }
    return "";
  }

  @Test
  public void testUtilization2RandomOrdering() throws InterruptedException {

    Utilization2RandomQueueOrderingPolicy policy =
        new Utilization2RandomQueueOrderingPolicy();

    // Case 1, 2 queues
    List<CSQueue>
        queues_1 = mockCSQueues(new String[] {"a", "b"},
        new float[] {1.1f, 0.0f}, "");
    queues_1.get(0).getQueueResourceUsage()
        .setPending("", Resource.newInstance(0, 1));
    queues_1.get(1).getQueueResourceUsage()
        .setPending("", Resource.newInstance(0, 1));

    policy.setQueues(queues_1);
    verifyOrder(policy, "", null, new String[] {"b", "a"});

    // Case 2, 3 queues with different util, will first choose c
    List<CSQueue>
        queues_2_1 = mockCSQueues(new String[] {"a", "b"},
        new float[] {1.5f, 1.1f}, "");
    List<CSQueue>
        queues_2_2 = mockCSQueues(new String[] {"c"},
        new float[] {0.1f}, "");
    List<CSQueue> queues_2 = new ArrayList<>(queues_2_1);
    queues_2.addAll(queues_2_2);

    queues_2.get(0).getQueueResourceUsage()
        .setPending("", Resource.newInstance(0, 1));
    queues_2.get(1).getQueueResourceUsage()
        .setPending("", Resource.newInstance(0, 1));
    queues_2.get(2).getQueueResourceUsage()
        .setPending("x", Resource.newInstance(0, 1));

    policy.setQueues(queues_2);
    Assert.assertEquals("c", getFirstOne(policy, "", new HashSet<>(
        Collections.singleton("x"))));

    // Case 3, 3 queues, though a&c is lowest utilization
    // will first choose b due to only b has pending resource
    List<CSQueue>
        queues_3_1 = mockCSQueues(new String[] {"a", "b"},
        new float[] {0.5f, 0.8f}, "");
    List<CSQueue>
        queues_3_2 = mockCSQueues(new String[] {"c"},
        new float[] {0.1f}, "");
    List<CSQueue> queues_3 = new ArrayList<>(queues_3_1);
    queues_3.addAll(queues_3_2);

    queues_3.get(0).getQueueResourceUsage()
        .setPending("", Resource.newInstance(0, 0));
    queues_3.get(1).getQueueResourceUsage()
        .setPending("", Resource.newInstance(0, 1));
    queues_3.get(2).getQueueResourceUsage()
        .setPending("x", Resource.newInstance(0, 0));

    policy.setQueues(queues_3);
    Assert.assertEquals("b", getFirstOne(policy, "", new HashSet<>(
        Collections.singleton("x"))));

  }
}
