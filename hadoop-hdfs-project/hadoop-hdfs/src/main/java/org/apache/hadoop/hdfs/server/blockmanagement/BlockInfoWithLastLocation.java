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

package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BlockInfoWithLastLocation {

  /** The original BlockInfo **/
  private final BlockInfo blk;
  /** The datanodeInfos of this high-risk block **/
  private final List<DatanodeStorageInfo> storages;

  /**
   * Build the BlockInfoWithLocation with the non-corrupt BlockInfo.
   */
  BlockInfoWithLastLocation(BlockInfo blk) {
    this.blk = blk;
    this.storages = new ArrayList<>();
    for (Iterator<DatanodeStorageInfo> it = blk.getStorageInfos(); it.hasNext(); ) {
      DatanodeStorageInfo storageInfo = it.next();
      this.storages.add(storageInfo);
    }
  }

  /**
   * Build the BlockInfoWithLocation with the corrupted BlockInfo.
   */
  BlockInfoWithLastLocation(BlockInfo blk, List<DatanodeStorageInfo> newStorages) {
    this.blk = blk;
    this.storages = newStorages == null ? new ArrayList<>() : new ArrayList<>(newStorages);
  }

  public List<DatanodeStorageInfo> getStorages() {
    return this.storages;
  }

  @Override
  public int hashCode() {
    return this.blk.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return (this == obj) || this.blk.equals(obj);
  }
}
