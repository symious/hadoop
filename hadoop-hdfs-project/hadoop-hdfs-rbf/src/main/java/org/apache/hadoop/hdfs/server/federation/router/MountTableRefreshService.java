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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.store.MountTableStore;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreUnavailableException;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreUtils;
import org.apache.hadoop.hdfs.server.federation.store.records.RouterState;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.service.AbstractService;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Update mount table cache whenever there is a change in mount table entries
 */
public class MountTableRefreshService extends AbstractService {
  private static final Logger LOG =
      LoggerFactory.getLogger(MountTableRefreshService.class);

  /** Router whose mount table cache will be refreshed */
  private final Router router;
  private MountTableStore mountTableStore;
  private String localAdminAdress;
  // how long to wait for all the router admins to finish their cache update
  private int maxUpdateTime;

  /**
   * All router admin clients cached. so no need to create the client again and
   * again.
   */
  private static Map<String, RouterClient> addressClientMap =
      new HashMap<String, RouterClient>();

  /**
   * Create a new service to refresh mount table cache when there is change in
   * mount table entries
   *
   * @param router whose mount table cache will be refreshed
   */
  public MountTableRefreshService(Router router) {
    super(MountTableRefreshService.class.getSimpleName());
    this.router = router;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    this.mountTableStore = getMountTableStore();
    // attach this service to mount table store.
    this.mountTableStore.setRefreshService(this);
    this.localAdminAdress =
        StateStoreUtils.getHostPortString(router.getAdminServerAddress());
    this.maxUpdateTime = conf.getInt(
        RBFConfigKeys.MOUNT_TABLE_CACHE_IMMEDIATE_UPDATE_MAX_TIME,
        RBFConfigKeys.MOUNT_TABLE_CACHE_IMMEDIATE_UPDATE_MAX_TIME_DEFAULT);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
    // close all admin clients
    Set<Entry<String, RouterClient>> entrySet = addressClientMap.entrySet();
    for (Entry<String, RouterClient> entry : entrySet) {
      try {
        entry.getValue().close();
      } catch (IOException e) {
        LOG.error("Error while closing RouterClient", e);
      }
    }
  }

  private MountTableStore getMountTableStore() throws IOException {
    MountTableStore mountTableStore =
        router.getStateStore().getRegisteredRecordStore(MountTableStore.class);
    if (mountTableStore == null) {
      throw new IOException("Mount table state store is not available.");
    }
    return mountTableStore;
  }

  /**
   * Refresh mount table cache of this router as well as all the routers
   */
  public void refresh() throws StateStoreUnavailableException {
    List<RouterState> cachedRecords =
        router.getRouterStateManager().getCachedRecords();
    List<MountTableRefreshThread> refreshThreads =
        new ArrayList<MountTableRefreshThread>();
    for (RouterState routerState : cachedRecords) {
      String adminAddress = routerState.getAdminAddress();
      if (adminAddress == null || adminAddress.length() == 0) {
        // this router has not enabled router admin
        continue;
      }
      RouterClient client = addressClientMap.get(adminAddress);
      if (client == null) {
        InetSocketAddress routerSocket =
            NetUtils.createSocketAddr(adminAddress);
        try {
          client = new RouterClient(routerSocket, getConfig());
        } catch (IOException e) {
          Log.warn("Failed to connect to router admin " + adminAddress
              + "Refresh mount table cache can not invoked", e);
          continue;
        }
        addressClientMap.put(adminAddress, client);
      }
      refreshThreads.add(new MountTableRefreshThread(client, mountTableStore,
          adminAddress, isLocalAdmin(adminAddress)));
    }
    invokeRefresh(refreshThreads);
  }

  private void invokeRefresh(List<MountTableRefreshThread> refreshThreads) {
    CountDownLatch countDownLatch = new CountDownLatch(refreshThreads.size());
    // wait for all the thread to complete
    for (MountTableRefreshThread refThread : refreshThreads) {
      refThread.setCountDownLatch(countDownLatch);
      refThread.start();
    }
    boolean allReqCompleted = false;
    try {
      // false if refresh is not finished within specified time
      allReqCompleted = countDownLatch.await(maxUpdateTime, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      LOG.error("InterruptedException", e);
    }

    logRestult(refreshThreads, allReqCompleted);
  }

  private boolean isLocalAdmin(String adminAddress) {
    return adminAddress.contentEquals(localAdminAdress);
  }

  private void logRestult(List<MountTableRefreshThread> refreshThreads,
      boolean allReqCompleted) {
    if (!allReqCompleted) {
      Log.warn("All router admins were not able to update their cache within "
          + maxUpdateTime + " minutes");
    }
    int succesCount = 0;
    int failureCount = 0;
    for (MountTableRefreshThread mountTableRefreshThread : refreshThreads) {
      if (mountTableRefreshThread.isSuccess()) {
        succesCount = succesCount + 1;
      } else {
        failureCount = failureCount + 1;
      }
    }
    Log.info("Mount table entries cache refresh succesCount=" + succesCount
        + ", failureCount=" + failureCount);
  }
}
