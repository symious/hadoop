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

package org.apache.hadoop.yarn.server.resourcemanager.nodelabels;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.nodelabels.RMNodeLabel;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeLabelsUpdateSchedulerEvent;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.collect.ImmutableSet;

public class RMNodeLabelsManager extends CommonNodeLabelsManager {
  protected static class Queue {
    private final Set<String> accessibleNodeLabels;
    private final Resource resource;
    private boolean any;

    protected Queue() {
      accessibleNodeLabels =
          Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
      resource = Resources.clone(Resources.none());
      any = false;
    }

    @Override
    public String toString() {
      return accessibleNodeLabels + " : " + resource;
    }

    /**
     * Return whether this queue would accept a host with the given label.
     * The {@code labels} parameter is a {@link Set} for historical reasons.
     * In practice the set size will always be less than 2.
     *
     * @param labels the host label as a set
     * @return whether the queue accepts the host
     */
    public boolean acceptLabels(Set<String> labels) {
      // node without any labels can be accessed by any queue
      return any || (labels == null) || labels.isEmpty() ||
          ((labels.size() == 1) && labels.contains(NO_LABEL)) ||
          !Collections.disjoint(accessibleNodeLabels, labels);
    }

    /**
     * Return whether this queue will accept all host labels.
     *
     * @return whether this queue will accept all host labels
     */
    @VisibleForTesting
    boolean acceptAny() {
      return any;
    }

    /**
     * Return whether this queue explicitly has the given label. If the queue
     * has only the {@link RMNodeLabelsManager#ANY} label, this method will
     * return true only when the {@code label} parameter is
     * {@link RMNodeLabelsManager#ANY}. If the {@code label} parameter is
     * {@link RMNodeLabelsManager#NO_LABEL}, then this method will only return
     * true if the queue has {@link RMNodeLabelsManager#NO_LABEL} in its set
     * of labels.
     *
     * @param label the label to test
     * @return whether the queue has the label
     */
    public boolean hasLabel(String label) {
      return accessibleNodeLabels.contains(label);
    }

    /**
     * Set the labels for this queue.
     *
     * @param labels the labels to set
     */
    public void setLabels(Set<String> labels) {
      accessibleNodeLabels.clear();
      accessibleNodeLabels.addAll(labels);
      any = hasLabel(ANY);
    }

    /**
     * Add the given resource to this queue's resource total.
     *
     * @param add the resource to add
     */
    public void addResource(Resource add) {
      Resources.addTo(resource, add);
    }

    /**
     * Subtract the given resource from this queue's resource total. The total
     * resource cannot be less than 0 for any resource type.
     *
     * @param subtract the resource to subtract
     */
    public void subtractResource(Resource subtract) {
      Resources.subtractFromNonNegative(resource, subtract);
    }

    /**
     * Return the total resources for this queue.
     *
     * @return the total resources for this queue
     */
    public Resource getResource() {
      return resource;
    }
  }

  private final ConcurrentMap<String, Queue> queueCollections =
      new ConcurrentHashMap<>();
  private YarnAuthorizationProvider authorizer;
  private RMContext rmContext = null;
  
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    authorizer = YarnAuthorizationProvider.getInstance(conf);
  }

  @Override
  public void addLabelsToNode(Map<NodeId, Set<String>> addedLabelsToNode)
      throws IOException {    
    try {
      writeLock.lock();

      // get nodesCollection before edition
      Map<String, Host> before = cloneNodeMap(addedLabelsToNode.keySet());

      super.addLabelsToNode(addedLabelsToNode);

      // get nodesCollection after edition
      Map<String, Host> after = cloneNodeMap(addedLabelsToNode.keySet());

      // update running nodes resources
      updateResourceMappings(before, after);
    } finally {
      writeLock.unlock();
    }
  }

  protected void checkRemoveFromClusterNodeLabelsOfQueue(
      Collection<String> labelsToRemove) throws IOException {
    // Check if label to remove doesn't existed or null/empty, will throw
    // exception if any of labels to remove doesn't meet requirement
    for (String label : labelsToRemove) {
      label = normalizeLabel(label);

      // check if any queue contains this label
      for (Entry<String, Queue> entry : queueCollections.entrySet()) {
        String queueName = entry.getKey();

        if (entry.getValue().hasLabel(label)) {
          throw new IOException("Cannot remove label=" + label
              + ", because queue=" + queueName + " is using this label. "
              + "Please remove label on queue before remove the label");
        }
      }
    }
  }

  @Override
  public void removeFromClusterNodeLabels(Collection<String> labelsToRemove)
      throws IOException {
    try {
      writeLock.lock();
      if (!isInitNodeLabelStoreInProgress()) {
        // We cannot remove node labels from collection when some queue(s) are
        // using any of them.
        // We will not do remove when recovery is in prpgress. During
        // service starting, we will replay edit logs and recover state. It is
        // possible that a history operation removed some labels which were not
        // used by some queues in the past but are used by current queues.
        checkRemoveFromClusterNodeLabelsOfQueue(labelsToRemove);
      }
      // copy before NMs
      Map<String, Host> before = cloneNodeMap();

      super.removeFromClusterNodeLabels(labelsToRemove);

      updateResourceMappings(before, nodeCollections);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void addToCluserNodeLabels(Collection<NodeLabel> labels)
      throws IOException {
    try {
      writeLock.lock();
      super.addToCluserNodeLabels(labels);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void
      removeLabelsFromNode(Map<NodeId, Set<String>> removeLabelsFromNode)
          throws IOException {
    try {
      writeLock.lock();

      // get nodesCollection before edition
      Map<String, Host> before =
          cloneNodeMap(removeLabelsFromNode.keySet());

      super.removeLabelsFromNode(removeLabelsFromNode);

      // get nodesCollection before edition
      Map<String, Host> after = cloneNodeMap(removeLabelsFromNode.keySet());

      // update running nodes resources
      updateResourceMappings(before, after);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void replaceLabelsOnNode(Map<NodeId, Set<String>> replaceLabelsToNode)
      throws IOException {
    try {
      writeLock.lock();

      Map<NodeId, Set<String>> effectiveModifiedLabelMappings =
          getModifiedNodeLabelsMappings(replaceLabelsToNode);

      if(effectiveModifiedLabelMappings.isEmpty()) {
        LOG.info("No Modified Node label Mapping to replace");
        return;
      }

      // get nodesCollection before edition
      Map<String, Host> before =
          cloneNodeMap(effectiveModifiedLabelMappings.keySet());

      super.replaceLabelsOnNode(effectiveModifiedLabelMappings);

      // get nodesCollection after edition
      Map<String, Host> after =
          cloneNodeMap(effectiveModifiedLabelMappings.keySet());

      // update running nodes resources
      updateResourceMappings(before, after);
    } finally {
      writeLock.unlock();
    }
  }

  private Map<NodeId, Set<String>> getModifiedNodeLabelsMappings(
      Map<NodeId, Set<String>> replaceLabelsToNode) {
    Map<NodeId, Set<String>> effectiveModifiedLabels = new HashMap<>();
    for (Entry<NodeId, Set<String>> nodeLabelMappingEntry : replaceLabelsToNode
        .entrySet()) {
      NodeId nodeId = nodeLabelMappingEntry.getKey();
      Set<String> modifiedNodeLabels = nodeLabelMappingEntry.getValue();
      Set<String> labelsBeforeModification = null;
      Host host = nodeCollections.get(nodeId.getHost());
      if (host == null) {
        effectiveModifiedLabels.put(nodeId, modifiedNodeLabels);
        continue;
      } else if (nodeId.getPort() == WILDCARD_PORT) {
        labelsBeforeModification = host.labels;
      } else if (host.nms.get(nodeId) != null) {
        labelsBeforeModification = host.nms.get(nodeId).labels;
      }
      if (labelsBeforeModification == null
          || labelsBeforeModification.size() != modifiedNodeLabels.size()
          || !labelsBeforeModification.containsAll(modifiedNodeLabels)) {
        effectiveModifiedLabels.put(nodeId, modifiedNodeLabels);
      }
    }
    return effectiveModifiedLabels;
  }

  /*
   * Following methods are used for setting if a node is up and running, and it
   * will update running nodes resource
   */
  public void activateNode(NodeId nodeId, Resource resource) {
    try {
      writeLock.lock();
      
      // save if we have a node before
      Map<String, Host> before = cloneNodeMap(ImmutableSet.of(nodeId));
      
      createHostIfNonExisted(nodeId.getHost());
      try {
        createNodeIfNonExisted(nodeId);
      } catch (IOException e) {
        LOG.error("This shouldn't happen, cannot get host in nodeCollection"
            + " associated to the node being activated");
        return;
      }

      Node nm = getNMInNodeSet(nodeId);
      nm.resource = resource;
      nm.running = true;

      // Add node in labelsCollection
      Set<String> labelsForNode = getLabelsByNode(nodeId);
      if (labelsForNode != null) {
        for (String label : labelsForNode) {
          RMNodeLabel labelInfo = labelCollections.get(label);
          if(labelInfo != null) {
            labelInfo.addNodeId(nodeId);
          }
        }
      }
      
      // get the node after edition
      Map<String, Host> after = cloneNodeMap(ImmutableSet.of(nodeId));
      
      updateResourceMappings(before, after);
    } finally {
      writeLock.unlock();
    }
  }
  
  /*
   * Following methods are used for setting if a node unregistered to RM
   */
  public void deactivateNode(NodeId nodeId) {
    try {
      writeLock.lock();
      
      // save if we have a node before
      Map<String, Host> before = cloneNodeMap(ImmutableSet.of(nodeId));
      Node nm = getNMInNodeSet(nodeId);
      if (null != nm) {
        if (null == nm.labels) {
          // When node deactivated, remove the nm from node collection if no
          // labels explicitly set for this particular nm

          // Save labels first, we need to remove label->nodes relation later
          Set<String> savedNodeLabels = getLabelsOnNode(nodeId);
          
          // Remove this node in nodes collection
          nodeCollections.get(nodeId.getHost()).nms.remove(nodeId);
          
          // Remove this node in labels->node
          removeNodeFromLabels(nodeId, savedNodeLabels);
        } else {
          // set nm is not running, and its resource = 0
          nm.running = false;
          nm.resource = Resource.newInstance(0, 0);
        }
      }
      
      // get the node after edition
      Map<String, Host> after = cloneNodeMap(ImmutableSet.of(nodeId));
      
      updateResourceMappings(before, after);
    } finally {
      writeLock.unlock();
    }
  }

  public void updateNodeResource(NodeId node, Resource newResource) {
    deactivateNode(node);
    activateNode(node, newResource);
  }

  public void reinitializeQueueLabels(Map<String, Set<String>> queueToLabels) {
    try {
      writeLock.lock();
      // clear before set
      this.queueCollections.clear();

      for (Entry<String, Set<String>> entry : queueToLabels.entrySet()) {
        addQueue(entry.getKey(), entry.getValue());
      }
    } finally {
      writeLock.unlock();
    }
  }

  public void addQueue(String queue, Set<String> labels) {
    Queue q = new Queue();
    writeLock.lock();

    try {
      queueCollections.put(queue, q);

      if ((labels != null) && !labels.isEmpty()) {
        q.setLabels(labels);
      }

      for (Host host : nodeCollections.values()) {
        for (Entry<NodeId, Node> nentry : host.nms.entrySet()) {
          NodeId nodeId = nentry.getKey();
          Node nm = nentry.getValue();

          if (nm.running && q.acceptLabels(getLabelsByNode(nodeId))) {
            q.addResource(nm.resource);
          }
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Return the total resources available to this queue according to its node
   * labels. If the queue cannot be found, no resources will be returned.
   *
   * @param queueName the name of the queue whose accessible resources will be
   * returned
   * @param queueLabels IGNORED
   * @param clusterResource IGNORED
   * @return the accessible resources for the queue
   */
  public Resource getQueueResource(String queueName, Set<String> queueLabels,
      Resource clusterResource) {
    try {
      readLock.lock();

      Queue q = queueCollections.get(queueName);

      if (q == null) {
        return Resources.none();
      } else {
        return q.getResource();
      }
    } finally {
      readLock.unlock();
    }
  }
  
  /*
   * Get active node count based on label.
   */
  public int getActiveNMCountPerLabel(String label) {
    if (label == null) {
      return 0;
    }
    try {
      readLock.lock();
      RMNodeLabel labelInfo = labelCollections.get(label);
      return (labelInfo == null) ? 0 : labelInfo.getNumActiveNMs();
    } finally {
      readLock.unlock();
    }
  }

  public Set<String> getLabelsOnNode(NodeId nodeId) {
    try {
      readLock.lock();
      Set<String> nodeLabels = getLabelsByNode(nodeId);
      return Collections.unmodifiableSet(nodeLabels);
    } finally {
      readLock.unlock();
    }
  }
  
  public boolean containsNodeLabel(String label) {
    try {
      readLock.lock();
      return label != null
          && (label.isEmpty() || labelCollections.containsKey(label));
    } finally {
      readLock.unlock();
    }
  }

  private Map<String, Host> cloneNodeMap(Set<NodeId> nodesToCopy) {
    Map<String, Host> map = new HashMap<String, Host>();
    for (NodeId nodeId : nodesToCopy) {
      if (!map.containsKey(nodeId.getHost())) {
        Host originalN = nodeCollections.get(nodeId.getHost());
        if (null == originalN) {
          continue;
        }
        Host n = originalN.copy();
        n.nms.clear();
        map.put(nodeId.getHost(), n);
      }

      Host n = map.get(nodeId.getHost());
      if (WILDCARD_PORT == nodeId.getPort()) {
        for (Entry<NodeId, Node> entry : nodeCollections
            .get(nodeId.getHost()).nms.entrySet()) {
          n.nms.put(entry.getKey(), entry.getValue().copy());
        }
      } else {
        Node nm = getNMInNodeSet(nodeId);
        if (null != nm) {
          n.nms.put(nodeId, nm.copy());
        }
      }
    }
    return map;
  }

  @SuppressWarnings("unchecked")
  private void updateResourceMappings(Map<String, Host> before,
      Map<String, Host> after) {
    // Get NMs in before only
    Set<NodeId> allNMs = new HashSet<NodeId>();
    for (Entry<String, Host> entry : before.entrySet()) {
      allNMs.addAll(entry.getValue().nms.keySet());
    }
    for (Entry<String, Host> entry : after.entrySet()) {
      allNMs.addAll(entry.getValue().nms.keySet());
    }
    
    // Map used to notify RM
    Map<NodeId, Set<String>> newNodeToLabelsMap = new HashMap<>();

    // traverse all nms
    for (NodeId nodeId : allNMs) {
      Node oldNM;
      if ((oldNM = getNMInNodeSet(nodeId, before, true)) != null) {
        Set<String> oldLabels = getLabelsByNode(nodeId, before);
        // no label in the past
        if (oldLabels.isEmpty()) {
          // update labels
          RMNodeLabel label = labelCollections.get(NO_LABEL);
          label.removeNode(oldNM.resource);

          // update queues, all queue can access this node
          for (Queue q : queueCollections.values()) {
            q.subtractResource(oldNM.resource);
          }
        } else {
          // update labels
          for (String labelName : oldLabels) {
            RMNodeLabel label = labelCollections.get(labelName);
            if (null == label) {
              continue;
            }
            label.removeNode(oldNM.resource);
          }

          // update queues, only queue can access this node will be subtract
          for (Queue q : queueCollections.values()) {
            if (q.acceptLabels(oldLabels)) {
              q.subtractResource(oldNM.resource);
            }
          }
        }
      }

      Node newNM;
      if ((newNM = getNMInNodeSet(nodeId, after, true)) != null) {
        Set<String> newLabels = getLabelsByNode(nodeId, after);
        
        newNodeToLabelsMap.put(nodeId, ImmutableSet.copyOf(newLabels));
        
        // no label now
        if (newLabels.isEmpty()) {
          // update labels
          RMNodeLabel label = labelCollections.get(NO_LABEL);
          label.addNode(newNM.resource);

          // update queues, all queue can access this node
          for (Queue q : queueCollections.values()) {
            q.addResource(newNM.resource);
          }
        } else {
          // update labels
          for (String labelName : newLabels) {
            RMNodeLabel label = labelCollections.get(labelName);
            label.addNode(newNM.resource);
          }

          // update queues, only queue can access this node will be subtract
          for (Queue q : queueCollections.values()) {
            if (q.acceptLabels(newLabels)) {
              q.addResource(newNM.resource);
            }
          }
        }
      }
    }
    
    // Notify RM
    if (rmContext != null && rmContext.getDispatcher() != null) {
      rmContext.getDispatcher().getEventHandler().handle(
          new NodeLabelsUpdateSchedulerEvent(newNodeToLabelsMap));
    }
  }

  /**
   * Get the total amount of resources available from active node managers that
   * have the given label. The resources for node managers without a label is
   * not included in this total unless the label is
   * {@link CommonNodeLabelsManager.NO_LABEL}.
   *
   * @param label the label for which to return resources
   * @param clusterResource IGNORED
   * @return the total resources associated with the label
   */
  public Resource getResourceByLabel(String label, Resource clusterResource) {
    label = normalizeLabel(label);
    if (label.equals(NO_LABEL)) {
      return noNodeLabel.getResource();
    }
    try {
      readLock.lock();
      RMNodeLabel nodeLabel = labelCollections.get(label);
      if (nodeLabel == null) {
        return Resources.none();
      }
      return nodeLabel.getResource();
    } finally {
      readLock.unlock();
    }
  }

  private Map<String, Host> cloneNodeMap() {
    Set<NodeId> nodesToCopy = new HashSet<NodeId>();
    for (String nodeName : nodeCollections.keySet()) {
      nodesToCopy.add(NodeId.newInstance(nodeName, WILDCARD_PORT));
    }
    return cloneNodeMap(nodesToCopy);
  }

  public boolean checkAccess(UserGroupInformation user) {
    // make sure only admin can invoke
    // this method
    if (authorizer.isAdmin(user)) {
      return true;
    }
    return false;
  }
  
  public void setRMContext(RMContext rmContext) {
    this.rmContext = rmContext;
  }

  public List<RMNodeLabel> pullRMNodeLabelsInfo() {
    try {
      readLock.lock();
      List<RMNodeLabel> infos = new ArrayList<RMNodeLabel>();

      for (Entry<String, RMNodeLabel> entry : labelCollections.entrySet()) {
        RMNodeLabel label = entry.getValue();
        infos.add(label.getCopy());
      }

      Collections.sort(infos);
      return infos;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public String toString() {
    StringBuilder out = new StringBuilder();

    out.append("LABELS:\n");
    for (Entry<String, RMNodeLabel> e : labelCollections.entrySet()) {
      out.append("\t").append(e.getKey()).append(": ");
      out.append(e.getValue().toString()).append("\n");
    }

    out.append("QUEUES:\n");
    for (Entry<String, Queue> e : queueCollections.entrySet()) {
      out.append("\t").append(e.getKey()).append(": ");
      out.append(e.getValue().toString()).append("\n");
    }

    out.append("NODES:\n");
    for (Entry<String, Host> e : nodeCollections.entrySet()) {
      out.append("\t").append(e.getKey()).append(": ");
      out.append(e.getValue().toString()).append("\n");
    }

    return out.toString();
  }
}
