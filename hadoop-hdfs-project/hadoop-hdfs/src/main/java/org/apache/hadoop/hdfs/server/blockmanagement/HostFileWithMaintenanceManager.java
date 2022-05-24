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
package org.apache.hadoop.hdfs.server.blockmanagement;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeID;

import java.io.IOException;

/**
 * This class extends HostFileManager to manages the include and exclude files for HDFS.
 * And override getMaintenanceExpirationTimeInMS and refresh methods to support maintenance.
 * <p/>
 * Provide DFS_HOSTS_MAINTENANCE_DOWNGRADE_DISABLED_KEY so that can downgrade dynamically liking HostFileManager.
 * <p/>
 */
public class HostFileWithMaintenanceManager extends HostFileManager {
  private HostSet maintenanceHosts = new HostSet();
  private long expirationTime = Long.MAX_VALUE;
  private boolean enabled = false;

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    enabled =
        getConf().getBoolean(DFSConfigKeys.DFS_HOSTS_MAINTENANCE_ENABLED_KEY,
            DFSConfigKeys.DFS_HOSTS_MAINTENANCE_ENABLED_DEFAULT);
  }

  @Override
  public void refresh() throws IOException {
    super.refresh();
    if (enabled) {
      //refresh maintenance state
      refreshMaintenance();
    } else {
      cleanupMaintenance();
    }
  }

  private void refreshMaintenance() throws IOException {
    refresh(getConf().get(DFSConfigKeys.DFS_HOSTS_MAINTENANCE, ""));
  }

  private void cleanupMaintenance() {
    synchronized (this) {
      maintenanceHosts = new HostSet();
    }
  }


  /**
   * Read the maintenance lists from the named files.  Any previous
   * maintenance lists are discarded.
   *
   * @param maintenanceFile the path to the new includes list
   * @throws IOException thrown if there is a problem reading one of the files
   */
  private void refresh(String maintenanceFile)
      throws IOException {
    HostSet newMaintenances = readFile("maintenance", maintenanceFile);
    refresh(newMaintenances);
  }


  /**
   * Set the newMaintenances lists by the new HostSet instances. The
   * old instances are discarded.
   *
   * @param newMaintenances the new maintenances list
   */
  @VisibleForTesting
  void refresh(HostSet newMaintenances) {
    synchronized (this) {
      maintenanceHosts = newMaintenances;
    }
  }

  @Override
  public long getMaintenanceExpirationTimeInMS(DatanodeID dn) {
    if (enabled) {
      synchronized (this) {
        if (!maintenanceHosts.match(dn.getResolvedAddress())) {
          return 0;
        }

        return expirationTime;
      }
    } else {
      return super.getMaintenanceExpirationTimeInMS(dn);
    }
  }

  public long getExpirationTime() {
    return expirationTime;
  }

  public void setExpirationTime(long expirationTime) {
    this.expirationTime = expirationTime;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }
}
