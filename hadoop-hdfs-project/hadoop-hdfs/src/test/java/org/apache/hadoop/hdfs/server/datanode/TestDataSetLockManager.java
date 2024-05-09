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

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.AutoCloseDataSetLock;
import org.apache.hadoop.hdfs.server.common.DataNodeLockManager.LockLevel;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestDataSetLockManager {
  private DataSetLockManager manager;

  @Before
  public void init() {
    Configuration conf = new Configuration();
    conf.setTimeDuration(
        DFSConfigKeys.DFS_DATANODE_LOCK_METRICS_THRESHOLD_MS_KEY, 100,
        TimeUnit.MILLISECONDS);
    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_LOCKMANAGER_TRACE, true);
    DataSetLockManager.resetMetrics();
    manager = new DataSetLockManager(conf);
  }

  @Test(timeout = 5000)
  public void testBaseFunc() {
    manager.addLock(LockLevel.BLOCK_POOl, "BPtest");
    manager.addLock(LockLevel.VOLUME, "BPtest", "Volumetest");

    AutoCloseDataSetLock lock = manager.writeLock(LockLevel.BLOCK_POOl, "BPtest");
    AutoCloseDataSetLock lock1 = manager.readLock(LockLevel.BLOCK_POOl, "BPtest");
    lock1.close();
    lock.close();

    manager.lockLeakCheck();
    assertNull(manager.getLastException());

    AutoCloseDataSetLock lock2 = manager.writeLock(LockLevel.VOLUME, "BPtest", "Volumetest");
    AutoCloseDataSetLock lock3 = manager.readLock(LockLevel.VOLUME, "BPtest", "Volumetest");
    lock3.close();
    lock2.close();

    manager.lockLeakCheck();
    assertNull(manager.getLastException());

    AutoCloseDataSetLock lock4 = manager.writeLock(LockLevel.BLOCK_POOl, "BPtest");
    AutoCloseDataSetLock lock5 = manager.readLock(LockLevel.VOLUME, "BPtest", "Volumetest");
    lock5.close();
    lock4.close();

    manager.lockLeakCheck();
    assertNull(manager.getLastException());

    manager.writeLock(LockLevel.VOLUME, "BPtest", "Volumetest");
    manager.lockLeakCheck();

    Exception lastException = manager.getLastException();
    assertEquals(lastException.getMessage(), "lock Leak");
  }

  @Test(timeout = 5000)
  public void testAcquireWriteLockError() throws InterruptedException {
    Thread t = new Thread(() -> {
      manager.readLock(LockLevel.BLOCK_POOl, "test");
      manager.writeLock(LockLevel.BLOCK_POOl, "test");
    });
    t.start();
    Thread.sleep(1000);
    manager.lockLeakCheck();
    Exception lastException = manager.getLastException();
    assertEquals(lastException.getMessage(), "lock Leak");
  }

  @Test(timeout = 5000)
  public void testLockLeakCheck() {
    manager.writeLock(LockLevel.BLOCK_POOl, "test");
    manager.lockLeakCheck();
    Exception lastException = manager.getLastException();
    assertEquals(lastException.getMessage(), "lock Leak");
  }

  @Test(timeout = 10000)
  public void testMetrics() throws InterruptedException {
    DataSetLockManager.resetMetrics();
    Thread t1, t2;
    int longHolds = 0;

    // Hold 2 short read locks that don't block each other
    t1 = holdLock(true, 1, LockLevel.BLOCK_POOl, "test");
    t2 = holdLock(true, 1, LockLevel.BLOCK_POOl, "test");
    // No long lock hold should have been recorded
    t1.join();
    t2.join();
    assertEquals(longHolds, DataSetLockManager.getDataSetLockMetrics().longHeldLocks.value());

    // Hold a short read lock then a short write lock
    t1 = holdLock(true, 1, LockLevel.BLOCK_POOl, "test");
    t2 = holdLock(false, 1, LockLevel.BLOCK_POOl, "test");
    // No long lock hold should have been recorded
    t1.join();
    t2.join();
    assertEquals(longHolds, DataSetLockManager.getDataSetLockMetrics().longHeldLocks.value());

    // Hold a long read lock that does not block a short read lock
    t1 = holdLock(true, 500, LockLevel.BLOCK_POOl, "test");
    t2 = holdLock(true, 1, LockLevel.BLOCK_POOl, "test");
    // No long lock hold should have been recorded
    t1.join();
    t2.join();
    assertEquals(longHolds, DataSetLockManager.getDataSetLockMetrics().longHeldLocks.value());

    // Hold a long read lock that blocks a short write lock
    t1 = holdLock(true, 500, LockLevel.BLOCK_POOl, "test");
    t2 = holdLock(false, 1, LockLevel.BLOCK_POOl, "test");
    // One long lock hold should have been recorded
    t1.join();
    t2.join();
    assertEquals(++longHolds, DataSetLockManager.getDataSetLockMetrics().longHeldLocks.value());

    // Hold a long write lock that blocks a short write lock
    t1 = holdLock(false, 500, LockLevel.BLOCK_POOl, "test");
    t2 = holdLock(false, 1, LockLevel.BLOCK_POOl, "test");
    // One long lock hold should have been recorded
    t1.join();
    t2.join();
    assertEquals(++longHolds, DataSetLockManager.getDataSetLockMetrics().longHeldLocks.value());
  }

  private Thread holdLock(final boolean isReadLock, final long duration,
      final LockLevel ll, final String... args) {
    Thread t = new Thread(() -> {
      if (isReadLock) {
        try (AutoCloseDataSetLock lock = manager.readLock(ll, args)) {
          Thread.sleep(duration);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      } else {
        try (AutoCloseDataSetLock lock = manager.writeLock(ll, args)) {
          Thread.sleep(duration);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    });
    t.start();
    return t;
  }
}
