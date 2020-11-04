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
package org.apache.hadoop.hdfs.server.federation.router;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.hdfs.server.federation.store.MountTableStore;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RefreshMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RefreshMountTableEntriesResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Refresh mount table cache of local or remote router admin
 */
public class MountTableRefreshThread extends Thread {
  private static final Logger LOG =
      LoggerFactory.getLogger(MountTableRefreshThread.class);
  private RouterClient client;
  private boolean success;
  // admin server on which refreshed to be invoked
  private String adminAddress;
  private MountTableStore mountTableStore;
  // true if this admin need to refresh its own mount table cache.
  private boolean localAdmin;
  private CountDownLatch countDownLatch;

  public MountTableRefreshThread(RouterClient client,
      MountTableStore mountTableStore, String adminAddress,
      boolean localAdmin) {
    this.client = client;
    this.mountTableStore = mountTableStore;
    this.adminAddress = adminAddress;
    this.localAdmin = localAdmin;
    setName("MountTableRefresh_" + adminAddress);
    setDaemon(true);
  }

  /**
   * Refresh cache on local and remote router admin
   */
  @Override
  public void run() {
    try {
      if (localAdmin) {
        success = mountTableStore.loadCache(true);
      } else {
        RefreshMountTableEntriesRequest refreshReq =
            RefreshMountTableEntriesRequest.newInstance();
        RefreshMountTableEntriesResponse refreshMountTableEntries =
            client.getMountTableManager().refreshMountTableEntries(refreshReq);
        // refresh is success
        success = refreshMountTableEntries.getResult();
      }
    } catch (IOException e) {
      LOG.error("Failed to refresh mount table entries cache at router "
          + adminAddress);
    } finally {
      countDownLatch.countDown();
    }
  }

  /**
   * 
   * @return true if cache was refreshed successfully.
   */
  public boolean isSuccess() {
    return success;
  }

  public void setCountDownLatch(CountDownLatch countDownLatch) {
    this.countDownLatch = countDownLatch;
  }

  @Override
  public String toString() {
    return "MountTableRefreshThread [success=" + success + ", adminAddress="
        + adminAddress + ", localAdmin=" + localAdmin + "]";
  }
}
