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

package org.apache.hadoop.yarn.server.nodemanager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of the node failed containers monitor. It periodically tracks
 * the failed containers of the node and reports it to the NM.
 */
public class NodeFailedContainersMonitorImpl extends AbstractService implements
    NodeFailedContainersMonitor {

  /** Logging infrastructure. */
  final static Logger LOG =
       LoggerFactory.getLogger(NodeFailedContainersMonitorImpl.class);

  /** Interval to monitor the node failed containers. */
  private long monitoringInterval;
  /** Thread to monitor the node failed containers. */
  private MonitoringThread monitoringThread;

  /** Last failed sum containers of the node. */
  private int lastFailedSumContainers;

  /** Current failed sum containers of the node. */
  private int curFailedSumContainers;

  /** Failed containers of the node during a period. */
  private int periodFailedContainers;

  private Context nmContext;

  /**
   * Initialize the node failed containers monitor.
   */
  public NodeFailedContainersMonitorImpl(Context context) {
    super(NodeFailedContainersMonitorImpl.class.getName());
    this.monitoringThread = new MonitoringThread();
    this.nmContext = context;
  }

  /**
   * Initialize the service with the proper parameters.
   */
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    this.monitoringInterval =
        conf.getLong(YarnConfiguration.NM_FAILED_CONTAINERS_MON_INTERVAL_MS,
            YarnConfiguration.DEFAULT_NM_FAILED_CONTAINERS_MON_INTERVAL_MS);
    LOG.info("monitoringInterval : " + this.monitoringInterval);
  }

  /**
   * Check if we should be monitoring.
   * @return <em>true</em> if we can monitor the node failed containers.
   */
  private boolean isEnabled() {
    if (this.monitoringInterval <= 0) {
      LOG.info("Node failed containers monitoring interval is <=0. "
          + this.getClass().getName() + " is disabled.");
      return false;
    }
    return true;
  }

  /**
   * Start the thread that does the node failed containers monitoring.
   */
  @Override
  protected void serviceStart() throws Exception {
    if (this.isEnabled()) {
      this.monitoringThread.start();
    }
    super.serviceStart();
  }

  /**
   * Stop the thread that does the node failed containers monitoring.
   */
  @Override
  protected void serviceStop() throws Exception {
    if (this.isEnabled()) {
      this.monitoringThread.interrupt();
      try {
        this.monitoringThread.join(10 * 1000);
      } catch (InterruptedException e) {
        LOG.warn("Could not wait for the thread to join");
      }
    }
    super.serviceStop();
  }

  /**
   * Thread that monitors the failed containers of this node.
   */
  private class MonitoringThread extends Thread {
    /**
     * Initialize the node resource monitoring thread.
     */
    public MonitoringThread() {
      super("Node Failed containers Monitor");
      this.setDaemon(true);
    }

    /**
     * Periodically monitor last failed sum containers of the node.
     */
    @Override
    public void run() {
      while (true) {
        NodeManagerMetrics nmMetrics = nmContext.getNodeManagerMetrics();
        lastFailedSumContainers = curFailedSumContainers;
        curFailedSumContainers = nmMetrics.getFailedContainers();
        periodFailedContainers = curFailedSumContainers - lastFailedSumContainers;
        LOG.info("lastFailedSumContainers = " + lastFailedSumContainers +
            " ,curFailedSumContainers = " + curFailedSumContainers +
            " ,periodFailedContainers = " + periodFailedContainers);
        try {
          Thread.sleep(monitoringInterval);
        } catch (InterruptedException e) {
          LOG.warn(NodeFailedContainersMonitorImpl.class.getName()
              + " is interrupted. Exiting.");
          break;
        }
      }
    }
  }

  /**
   * Get the <em>failed containers</em> of the node during a period.
   * @return <em>failed containers</em> of the node during a period.
   */
  @Override
  public int getPeriodFailedContainers() {
    return this.periodFailedContainers;
  }

}
