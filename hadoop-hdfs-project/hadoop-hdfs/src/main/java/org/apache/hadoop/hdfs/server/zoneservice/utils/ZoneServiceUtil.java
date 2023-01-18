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
package org.apache.hadoop.hdfs.server.zoneservice.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.apache.hadoop.hdfs.server.zoneservice.web.resources.ResultCode;

import java.net.URI;
import java.util.Collection;
import java.util.Objects;

import static org.apache.hadoop.hdfs.server.balancer.ExitStatus.UNFINALIZED_UPGRADE;

public class ZoneServiceUtil {
  public static URI getNamespaceUri(String namespace, Configuration conf)
      throws IllegalArgumentException {
    Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
    for (URI namenode: namenodes) {
      if (namenode.getAuthority().equals(namespace)) {
        return namenode;
      }
    }
    throw new IllegalArgumentException(
        "Cannot find the NameNode for namespace: " + namespace);
  }

  public static ResultCode convertExitStatus2ResultCode(ExitStatus exitStatus) {
    switch (Objects.requireNonNull(exitStatus)) {
    case SUCCESS:
      return ResultCode.SUCCESS;
    case IN_PROGRESS:
      return ResultCode.IN_PROGRESS;
    case ALREADY_RUNNING:
      return ResultCode.ALREADY_RUNNING;
    case NO_MOVE_BLOCK:
      return ResultCode.NO_MOVE_BLOCK;
    case NO_MOVE_PROGRESS:
      return ResultCode.NO_MOVE_PROGRESS;
    case IO_EXCEPTION:
      return ResultCode.IO_EXCEPTION;
    case ILLEGAL_ARGUMENTS:
      return ResultCode.ILLEGAL_ARGUMENTS;
    case INTERRUPTED:
      return ResultCode.INTERRUPTED;
    case UNFINALIZED_UPGRADE:
      return ResultCode.UNFINALIZED_UPGRADE;
    }
    return ResultCode.UNKNOWNERROR;
  }
}