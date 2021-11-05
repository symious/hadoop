package org.apache.hadoop.hdfs.server.zoneservice;

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
}
