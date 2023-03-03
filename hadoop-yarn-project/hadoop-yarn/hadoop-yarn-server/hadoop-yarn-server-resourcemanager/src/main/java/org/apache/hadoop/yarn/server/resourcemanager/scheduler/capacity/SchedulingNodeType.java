package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

public enum SchedulingNodeType {
  /**
   * <p>
   * Old scheduling mode, triggered by the nodeUpdate() method.
   * </p>
   */
  HEARTBEAT,

  /**
   * <p>
   * New scheduling mode, triggered by AsyncScheduleThread.run().
   * </p>
   */
  GLOBAL
}
