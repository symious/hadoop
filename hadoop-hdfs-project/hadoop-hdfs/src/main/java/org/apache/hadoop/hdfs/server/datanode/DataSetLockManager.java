/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.datanode;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.AutoCloseDataSetLock;
import org.apache.hadoop.hdfs.server.common.DataNodeLockManager;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Class for maintain a set of lock for fsDataSetImpl.
 */
public class DataSetLockManager implements DataNodeLockManager<AutoCloseDataSetLock> {
  public static final Logger LOG =
      LoggerFactory.getLogger(DataSetLockManager.class);
  private final HashMap<String, TrackLog> threadCountMap = new HashMap<>();
  private final LockMap lockMap = new LockMap();
  private long lockLogThresholdNs = 0;
  private boolean isFair = true;
  private final boolean openLockTrace;
  private Exception lastException;
  private final Timer timer = new Timer();

  private static final Object object = new Object();
  private static DataSetLockMetrics metrics;

  @Metrics(about = "Dataset lock metrics", context = "datasetlock")
  static class DataSetLockMetrics {
    final MetricsRegistry registry = new MetricsRegistry("DatasetLockMetrics");

    @Metric("Rate of dataset lock (nanoseconds)")
    MutableRate lockNs;

    @Metric("Rate of long held lock")
    MutableCounterLong longHeldLocks;

    static DataSetLockMetrics create() {
      return DefaultMetricsSystem.instance().register(new DataSetLockMetrics());
    }
  }

  @VisibleForTesting
  static void resetMetrics() {
    DefaultMetricsSystem.instance().unregisterSource("DataSetLockMetrics");
  }

  @VisibleForTesting
  static DataSetLockMetrics getDataSetLockMetrics() {
    return metrics;
  }


  /**
   * Class for maintain lockMap and is thread safe.
   */
  private class LockMap {
    private final HashMap<String, AutoCloseDataSetLock> readLockMap = new HashMap<>();
    private final HashMap<String, AutoCloseDataSetLock> writeLockMap = new HashMap<>();

    public synchronized void addLock(String name, ReentrantReadWriteLock lock) {
      AutoCloseDataSetLock readLock = new AutoCloseDataSetLock(lock.readLock());
      AutoCloseDataSetLock writeLock = new AutoCloseDataSetLock(lock.writeLock());
      if (openLockTrace) {
        readLock.setDataNodeLockManager(DataSetLockManager.this);
        writeLock.setDataNodeLockManager(DataSetLockManager.this);
      }

      // HDFS-16429: readlockMap.putIfAbsent(name, readLock);
      if (!readLockMap.containsKey(name)) {
        readLockMap.put(name, readLock);
      }

      // HDFS-16429: writeLockMap.putIfAbsent(name, writeLock);
      if (!writeLockMap.containsKey(name)) {
        writeLockMap.put(name, writeLock);
      }
    }

    public synchronized void removeLock(String name) {
      if (!readLockMap.containsKey(name) || !writeLockMap.containsKey(name)) {
        LOG.error("The lock {} is not in LockMap", name);
      }
      readLockMap.remove(name);
      writeLockMap.remove(name);
    }

    public synchronized AutoCloseDataSetLock getReadLock(String name) {
      return readLockMap.get(name);
    }

    public synchronized AutoCloseDataSetLock getWriteLock(String name) {
      return writeLockMap.get(name);
    }
  }

  /**
   * Generate lock order string concatenates with lock name.
   * @param level which level lock want to acquire.
   * @param resources lock name by lock order.
   * @return lock order string concatenates with lock name.
   */
  private String generateLockName(LockLevel level, String... resources) {
    if (resources.length == 1 && level == LockLevel.BLOCK_POOl) {
      if (resources[0] == null) {
        throw new IllegalArgumentException("acquire a null block pool lock");
      }
      return resources[0];
    } else if (resources.length == 2 && level == LockLevel.VOLUME) {
      if (resources[0] == null || resources[1] == null) {
        throw new IllegalArgumentException("acquire a null bp lock : "
            + resources[0] + "volume lock :" + resources[1]);
      }
      return resources[0] + resources[1];
    } else {
      throw new IllegalArgumentException("lock level do not match resource");
    }
  }

  /**
   * Class for record thread acquire lock stack trace and count.
   */
  private static class TrackLog {
    private final Stack<Exception> logStack = new Stack<>();
    private int lockCount = 0;
    private final String threadName;

    TrackLog(String threadName) {
      this.threadName = threadName;
      incrLockCount();
    }

    public void incrLockCount() {
      logStack.push(new Exception("lock stack trace"));
      lockCount += 1;
    }

    public void decrLockCount() {
      logStack.pop();
      lockCount -= 1;
    }

    public void showLockMessage() {
      LOG.error("hold lock thread name is: {} hold count is: {}", threadName,
          lockCount);
      while (!logStack.isEmpty()) {
        Exception e = logStack.pop();
        LOG.error("lock stack ", e);
      }
    }

    public boolean shouldClear() {
      return lockCount == 1;
    }
  }

  void createMetric() {
    synchronized (object) {
      if (metrics == null) {
        metrics = DataSetLockMetrics.create();
      }
    }
  }

  public DataSetLockManager(Configuration conf) {
    this.isFair = conf.getBoolean(
        DFSConfigKeys.DFS_DATANODE_LOCK_FAIR_KEY,
        DFSConfigKeys.DFS_DATANODE_LOCK_FAIR_DEFAULT);
    this.openLockTrace = conf.getBoolean(
        DFSConfigKeys.DFS_DATANODE_LOCKMANAGER_TRACE,
        DFSConfigKeys.DFS_DATANODE_LOCKMANAGER_TRACE_DEFAULT);
    long lockLogThresholdMs = conf.getTimeDuration(
        DFSConfigKeys.DFS_DATANODE_LOCK_METRICS_THRESHOLD_MS_KEY,
        DFSConfigKeys.DFS_DATANODE_LOCK_METRICS_THRESHOLD_MS_DEFAULT,
        TimeUnit.MILLISECONDS);
    this.lockLogThresholdNs = 1000000 * lockLogThresholdMs;
    createMetric();
  }

  public DataSetLockManager() {
    this.openLockTrace = true;
    createMetric();
  }

  @Override
  public AutoCloseDataSetLock readLock(LockLevel level, String... resources) {
    if (level == LockLevel.BLOCK_POOl) {
      return getReadLock(level, resources[0]);
    } else {
      AutoCloseDataSetLock bpLock = getReadLock(LockLevel.BLOCK_POOl, resources[0]);
      AutoCloseDataSetLock volLock = getReadLock(level, resources);
      volLock.setParentLock(bpLock);
      if (openLockTrace) {
        LOG.info("Sub lock {} {} parent lock {}", resources[0], resources[1], resources[0]);
      }
      return volLock;
    }
  }

  @Override
  public AutoCloseDataSetLock writeLock(LockLevel level, String... resources) {
    if (level == LockLevel.BLOCK_POOl) {
      return getWriteLock(level, resources[0]);
    } else {
      AutoCloseDataSetLock bpLock = getReadLock(LockLevel.BLOCK_POOl, resources[0]);
      AutoCloseDataSetLock volLock = getWriteLock(level, resources);
      volLock.setParentLock(bpLock);
      if (openLockTrace) {
        LOG.info("Sub lock {} {} parent lock {}", resources[0], resources[1], resources[0]);
      }
      return volLock;
    }
  }

  /**
   * Return a not null ReadLock.
   */
  private AutoCloseDataSetLock getReadLock(LockLevel level, String... resources) {
    String lockName = generateLockName(level, resources);
    AutoCloseDataSetLock lock = lockMap.getReadLock(lockName);
    if (lock == null) {
      LOG.warn("Ignore this error during dn restart: Not existing readLock {}.", lockName);
      lockMap.addLock(lockName, new ReentrantReadWriteLock(isFair));
      lock = lockMap.getReadLock(lockName);
    }
    lockWithMetrics(lock);
    if (openLockTrace) {
      putThreadName(getThreadName());
    }
    return lock;
  }

  /**
   * Return a not null WriteLock.
   */
  private AutoCloseDataSetLock getWriteLock(LockLevel level, String... resources) {
    String lockName = generateLockName(level, resources);
    AutoCloseDataSetLock lock = lockMap.getWriteLock(lockName);
    if (lock == null) {
      LOG.warn("Ignore this error during dn restart: Not existing writeLock {}.", lockName);
      lockMap.addLock(lockName, new ReentrantReadWriteLock(isFair));
      lock = lockMap.getWriteLock(lockName);
    }
    lockWithMetrics(lock);
    if (openLockTrace) {
      putThreadName(getThreadName());
    }
    return lock;
  }

  private void lockWithMetrics(AutoCloseDataSetLock lock) {
    long startNanos = timer.monotonicNowNanos();
    lock.lock();
    long nowNs = timer.monotonicNowNanos();
    long waitDurationNs = nowNs - startNanos;
    metrics.lockNs.add(waitDurationNs);
    if (waitDurationNs > lockLogThresholdNs) {
      metrics.longHeldLocks.incr();
    }
  }

  @Override
  public void addLock(LockLevel level, String... resources) {
    String lockName = generateLockName(level, resources);
    if (level == LockLevel.BLOCK_POOl) {
      lockMap.addLock(lockName, new ReentrantReadWriteLock(isFair));
    } else {
      // Should add block_pool lock first, then add bp+volume lock.
      String bpLockName = generateLockName(LockLevel.BLOCK_POOl, resources[0]);
      lockMap.addLock(bpLockName, new ReentrantReadWriteLock(isFair));
      lockMap.addLock(lockName, new ReentrantReadWriteLock(isFair));
    }
  }

  @Override
  public void removeLock(LockLevel level, String... resources) {
    String lockName = generateLockName(level, resources);
    try (AutoCloseDataSetLock lock = writeLock(level, resources)) {
      lockMap.removeLock(lockName);
    }
  }

  @Override
  public void hook() {
    if (openLockTrace) {
      removeThreadName(getThreadName());
    }
  }

  /**
   * Add thread name when lock a lock.
   */
  private synchronized void putThreadName(String thread) {
    if (threadCountMap.containsKey(thread)) {
      TrackLog trackLog = threadCountMap.get(thread);
      trackLog.incrLockCount();
    }

    // HDFS-16429: threadCountMap.putIfAbsent(thread, new TrackLog(thread));
    if (!threadCountMap.containsKey(thread)) {
      synchronized (threadCountMap) {
        if (!threadCountMap.containsKey(thread)) {
          threadCountMap.put(thread, new TrackLog(thread));
        }
      }
    }
  }

  public synchronized void lockLeakCheck() {

    if (!openLockTrace) {
      LOG.warn("not open lock leak check func");
      return;
    }
    if (threadCountMap.isEmpty()) {
      LOG.warn("all lock has release");
      return;
    }
    setLastException(new Exception("lock Leak"));
    //HDFS-16429: threadCountMap.forEach((name, trackLog) -> trackLog.showLockMessage());
    for (Map.Entry<String, TrackLog> entry : threadCountMap.entrySet()) {
      entry.getValue().showLockMessage();
    }
  }

  /**
   * Remove thread name when unlock a lock.
   */
  private synchronized void removeThreadName(String thread) {
    if (threadCountMap.containsKey(thread)) {
      TrackLog trackLog = threadCountMap.get(thread);
      if (trackLog.shouldClear()) {
        threadCountMap.remove(thread);
        return;
      }
      trackLog.decrLockCount();
    }
  }

  private void setLastException(Exception e) {
    this.lastException = e;
  }

  public Exception getLastException() {
    return lastException;
  }

  private String getThreadName() {
    return Thread.currentThread().getName() + Thread.currentThread().getId();
  }
}