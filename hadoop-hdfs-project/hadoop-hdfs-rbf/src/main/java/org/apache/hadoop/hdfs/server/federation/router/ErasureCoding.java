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

import org.apache.hadoop.hdfs.protocol.ECBlockGroupStats;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamespaceInfo;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Module that implements all the RPC calls in
 * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol} related to
 * Erasure Coding in the {@link RouterRpcServer}.
 */
public class ErasureCoding {

  /** RPC server to receive client calls. */
  private final RouterRpcServer rpcServer;
  /** RPC clients to connect to the Namenodes. */
  private final RouterRpcClient rpcClient;
  /** Interface to identify the active NN for a nameservice or blockpool ID. */
  private final ActiveNamenodeResolver namenodeResolver;

  public ErasureCoding(RouterRpcServer server) {
    this.rpcServer = server;
    this.rpcClient =  this.rpcServer.getRPCClient();
    this.namenodeResolver = this.rpcClient.getNamenodeResolver();
  }

  public ErasureCodingPolicy getErasureCodingPolicy(String src)
      throws IOException {
    rpcServer.checkOperation(OperationCategory.READ);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false, false);
    RemoteMethod remoteMethod = new RemoteMethod("getErasureCodingPolicy",
        new Class<?>[] {String.class}, new RemoteParam());
    return rpcClient.invokeSequential(
        locations, remoteMethod, null, null);
  }

  public void setErasureCodingPolicy(String src, String ecPolicyName)
      throws IOException {
    rpcServer.checkOperation(OperationCategory.WRITE);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false, false);
    RemoteMethod remoteMethod = new RemoteMethod("setErasureCodingPolicy",
        new Class<?>[] {String.class, String.class},
        new RemoteParam(), ecPolicyName);
    if (rpcServer.getClientProto().isPathAll(src)) {
      rpcClient.invokeConcurrent(locations, remoteMethod);
    } else {
      rpcClient.invokeSequential(locations, remoteMethod);
    }
  }

  public void unsetErasureCodingPolicy(String src) throws IOException {
    rpcServer.checkOperation(OperationCategory.WRITE);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false, false);
    RemoteMethod remoteMethod = new RemoteMethod("unsetErasureCodingPolicy",
        new Class<?>[] {String.class}, new RemoteParam());
    if (rpcServer.getClientProto().isPathAll(src)) {
      rpcClient.invokeConcurrent(locations, remoteMethod);
    } else {
      rpcClient.invokeSequential(locations, remoteMethod);
    }
  }

  public ECBlockGroupStats getECBlockGroupStats() throws IOException {
    rpcServer.checkOperation(OperationCategory.READ);

    RemoteMethod method = new RemoteMethod("getECBlockGroupStats");
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    Map<FederationNamespaceInfo, ECBlockGroupStats> allStats =
        rpcClient.invokeConcurrent(
            nss, method, true, false, ECBlockGroupStats.class);

    return ECBlockGroupStats.merge(allStats.values());
  }
}