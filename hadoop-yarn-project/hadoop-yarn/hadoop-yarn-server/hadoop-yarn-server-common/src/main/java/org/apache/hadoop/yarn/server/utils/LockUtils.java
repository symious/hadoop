package org.apache.hadoop.yarn.server.utils;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;

public class LockUtils {

  public static void tryLockUntilLocked(Lock lock) {
    while (!lock.tryLock()) {
      LockSupport.parkNanos(10000);
    }
  }

}
