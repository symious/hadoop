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
package org.apache.hadoop.hdfs.server.federation.fairness;

import org.apache.hadoop.hdfs.server.federation.utils.AdjustableSemaphore;
import java.util.concurrent.TimeUnit;

/**
 * A class that manages permits in an exclusive mode.
 */
public class StaticPermitManager implements AbstractPermitManager {
  private final String nsId;
  private volatile int permitCap;
  private final AdjustableSemaphore dedicatedPermits;

  public StaticPermitManager(final String nsId, final int permitCap) {
    this.nsId = nsId;
    this.permitCap = permitCap;
    this.dedicatedPermits = new AdjustableSemaphore(permitCap);
  }

  @Override
  public Permit acquirePermit() {
    Permit permit = Permit.NO_PERMIT;
    try {
      if (dedicatedPermits.tryAcquire(1, TimeUnit.SECONDS)) {
        permit = Permit.DEDICATED;
      }
    } catch (InterruptedException e) {
      // ignore
    }
    return permit;
  }

  @Override
  public void releasePermit(Permit permit) {
    if (permit == Permit.DEDICATED) {
      this.dedicatedPermits.release();
    }
  }

  @Override
  public void drainPermits() {
    this.dedicatedPermits.drainPermits();
  }

  @Override
  public int availablePermits() {
    return this.dedicatedPermits.availablePermits();
  }

  @Override
  public int getPermitCap() {
    return this.permitCap;
  }

  public void release(int permits) {
    this.dedicatedPermits.release(permits);
  }

  public void reducePermits(int reduction) {
    this.dedicatedPermits.reducePermits(reduction);
  }

  public void resetPermitCap(int permitCap) {
    this.permitCap = permitCap;
  }

  @Override
  public String toString() {
    return "StaticPermitManager, nsId=" + nsId
        + ", permitCap=" + permitCap
        + ", availablePermits=" + this.dedicatedPermits.availablePermits();
  }
}