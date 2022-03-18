package org.apache.hadoop.hdfs.server.zoneservice;

import org.apache.hadoop.fs.Path;

import java.util.List;

public abstract class ZoneMoverTrigger {
  /**
   * After check, if we should trigger ZoneMover for another round
   * @return true if we need trigger ZoneMover,
   *         false if we need to stop ZoneMover current thread
   */
  public abstract boolean hasNext();
  /**
   * Get next elements from trigger
   * @return String contain the content trigger need feed back to ZoneMover
   */
  public abstract String getNext() throws InterruptedException;

  /**
   * Update the care paths
   * @param paths the new path list
   */
  public abstract void updatePaths(List<Path> paths);
  /**
   * Close the thread and recycle the resources
   */
  public abstract void shutdown();
}
