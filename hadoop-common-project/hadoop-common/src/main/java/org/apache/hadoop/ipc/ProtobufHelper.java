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
package org.apache.hadoop.ipc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ipc.proto.RefreshCallQueueProtocolProtos.RefreshCallQueueTypeProto;
import org.apache.hadoop.ipc.RefreshCallQueueProtocol.RefreshCallQueueType;

import com.google.protobuf.ServiceException;

/**
 * Helper methods for protobuf related RPC implementation
 */
@InterfaceAudience.Private
public class ProtobufHelper {
  private ProtobufHelper() {
    // Hidden constructor for class with only static helper methods
  }

  /**
   * Return the IOException thrown by the remote server wrapped in 
   * ServiceException as cause.
   * @param se ServiceException that wraps IO exception thrown by the server
   * @return Exception wrapped in ServiceException or
   *         a new IOException that wraps the unexpected ServiceException.
   */
  public static IOException getRemoteException(ServiceException se) {
    Throwable e = se.getCause();
    if (e == null) {
      return new IOException(se);
    }
    return e instanceof IOException ? (IOException) e : new IOException(se);
  }

  public static EnumSet<RefreshCallQueueType> convertRefreshCallQueueTypes(
      List<RefreshCallQueueTypeProto> refreshCallQueueTypeProtos) {
    EnumSet<RefreshCallQueueType> types =
        EnumSet.noneOf(RefreshCallQueueType.class);
    for (RefreshCallQueueTypeProto typeProto : refreshCallQueueTypeProtos) {
      RefreshCallQueueType type =
          RefreshCallQueueType.valueOf((short)typeProto.getNumber());
      if (type != null) {
        types.add(type);
      }
    }
    return types;
  }

  public static List<RefreshCallQueueTypeProto> convertRefreshCallQueueTypes(
      EnumSet<RefreshCallQueueType> types) {
    List<RefreshCallQueueTypeProto> typeProtos = new ArrayList<>();
    for (RefreshCallQueueType type : types) {
      RefreshCallQueueTypeProto typeProto =
          RefreshCallQueueTypeProto.valueOf(type.getMode());
      if (typeProto != null) {
        typeProtos.add(typeProto);
      }
    }
    return typeProtos;
  }

}
