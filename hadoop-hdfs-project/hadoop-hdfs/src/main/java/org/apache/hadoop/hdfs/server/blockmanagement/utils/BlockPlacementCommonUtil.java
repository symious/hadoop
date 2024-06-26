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

package org.apache.hadoop.hdfs.server.blockmanagement.utils;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;

import java.util.List;

public class BlockPlacementCommonUtil {

  public static <T> DatanodeInfo getDatanodeInfo(T datanode) {
    Preconditions.checkArgument(datanode instanceof DatanodeInfo ||
            datanode instanceof DatanodeStorageInfo,
        "class " + datanode.getClass().getName() + " not allowed");
    if (datanode instanceof DatanodeInfo) {
      return ((DatanodeInfo)datanode);
    } else {
      return ((DatanodeStorageInfo)datanode).getDatanodeDescriptor();
    }
  }

  // Check if moving from source to target will reduce the number of
  // groups. The groups could be based on racks or upgrade domains.
  public static <T> boolean notReduceNumOfGroups(List<T> moreThanOne, T source, T target) {
    if (moreThanOne.contains(source)) {
      return true; // source and some other nodes are under the same group.
    } else if (target != null && !moreThanOne.contains(target)) {
      return true; // the added node adds a new group.
    }
    return false; // removing delHint reduces the number of groups.
  }
}