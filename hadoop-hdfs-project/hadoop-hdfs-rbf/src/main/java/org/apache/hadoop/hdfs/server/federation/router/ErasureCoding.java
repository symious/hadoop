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

import static org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer.merge;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hdfs.OperationName;
import org.apache.hadoop.hdfs.protocol.AddErasureCodingPolicyResponse;
import org.apache.hadoop.hdfs.protocol.ECBlockGroupStats;
import org.apache.hadoop.hdfs.protocol.ECTopologyVerifierResult;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamespaceInfo;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;

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

  public ErasureCodingPolicyInfo[] getErasureCodingPolicies()
      throws IOException {
    rpcServer.checkOperation(OperationCategory.READ);

    RemoteMethod method = new RemoteMethod("getErasureCodingPolicies");
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    Map<FederationNamespaceInfo, ErasureCodingPolicyInfo[]> ret;
    try {
      ret = rpcClient.invokeConcurrent(nss, method, true, false, ErasureCodingPolicyInfo[].class);
    } catch (IOException e) {
      this.rpcServer.getClientProtocolModule().
          logAuditEvent(false, OperationName.GET_ERASURE_CODING_POLICIES,
              this.rpcServer.getClientProtocolModule().INVOKE_TYPE_CONCURRENT);
      throw e;
    }
    this.rpcServer.getClientProtocolModule().
        logAuditEvent(true, OperationName.GET_ERASURE_CODING_POLICIES,
            this.rpcServer.getClientProtocolModule().INVOKE_TYPE_CONCURRENT);
    return merge(ret, ErasureCodingPolicyInfo.class);
  }

  public Map<String, String> getErasureCodingCodecs() throws IOException {
    rpcServer.checkOperation(OperationCategory.READ);

    RemoteMethod method = new RemoteMethod("getErasureCodingCodecs");
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    @SuppressWarnings("rawtypes")
    Map<FederationNamespaceInfo, Map> retCodecs;
    try {
      retCodecs = rpcClient.invokeConcurrent(nss, method, true, false, Map.class);
    } catch (IOException e) {
      this.rpcServer.getClientProtocolModule().
          logAuditEvent(false, OperationName.GET_ERASURE_CODING_CODECS,
              this.rpcServer.getClientProtocolModule().INVOKE_TYPE_CONCURRENT);
      throw e;
    }

    Map<String, String> ret = new HashMap<>();
    Object obj = retCodecs;
    @SuppressWarnings("unchecked")
    Map<FederationNamespaceInfo, Map<String, String>> results =
        (Map<FederationNamespaceInfo, Map<String, String>>)obj;
    Collection<Map<String, String>> allCodecs = results.values();
    for (Map<String, String> codecs : allCodecs) {
      ret.putAll(codecs);
    }
    this.rpcServer.getClientProtocolModule().
        logAuditEvent(true, OperationName.GET_ERASURE_CODING_CODECS,
            this.rpcServer.getClientProtocolModule().INVOKE_TYPE_CONCURRENT);
    return ret;
  }

  public AddErasureCodingPolicyResponse[] addErasureCodingPolicies(
      ErasureCodingPolicy[] policies) throws IOException {
    rpcServer.checkOperation(OperationCategory.WRITE);

    RemoteMethod method = new RemoteMethod("addErasureCodingPolicies",
        new Class<?>[] {ErasureCodingPolicy[].class}, new Object[] {policies});
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    Map<FederationNamespaceInfo, AddErasureCodingPolicyResponse[]> ret;
    try {
      ret = rpcClient.invokeConcurrent(nss, method, true, false, AddErasureCodingPolicyResponse[].class);
    } catch (IOException e) {
      this.rpcServer.getClientProtocolModule().
          logAuditEvent(false, OperationName.ADD_ERASURE_CODING_POLICIES,
              this.rpcServer.getClientProtocolModule().INVOKE_TYPE_CONCURRENT);
      throw e;
    }
    this.rpcServer.getClientProtocolModule().
        logAuditEvent(true, OperationName.ADD_ERASURE_CODING_POLICIES,
            this.rpcServer.getClientProtocolModule().INVOKE_TYPE_CONCURRENT);
    return merge(ret, AddErasureCodingPolicyResponse.class);
  }

  public void removeErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    rpcServer.checkOperation(OperationCategory.WRITE);

    RemoteMethod method = new RemoteMethod("removeErasureCodingPolicy",
        new Class<?>[] {String.class}, ecPolicyName);
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    try {
      rpcClient.invokeConcurrent(nss, method, true, false);
    } catch (IOException e) {
      this.rpcServer.getClientProtocolModule().
          logAuditEvent(false, OperationName.REMOVE_ERASURE_CODING_POLICY,
              this.rpcServer.getClientProtocolModule().INVOKE_TYPE_CONCURRENT);
      throw e;
    }
    this.rpcServer.getClientProtocolModule().
        logAuditEvent(true, OperationName.REMOVE_ERASURE_CODING_POLICY,
            this.rpcServer.getClientProtocolModule().INVOKE_TYPE_CONCURRENT);
  }

  public void disableErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    rpcServer.checkOperation(OperationCategory.WRITE);

    RemoteMethod method = new RemoteMethod("disableErasureCodingPolicy",
        new Class<?>[] {String.class}, ecPolicyName);
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    try {
      rpcClient.invokeConcurrent(nss, method, true, false);
    } catch (IOException e) {
      this.rpcServer.getClientProtocolModule().
          logAuditEvent(false, OperationName.DISABLE_ERASURE_CODING_POLICY,
              this.rpcServer.getClientProtocolModule().INVOKE_TYPE_CONCURRENT);
      throw e;
    }
    this.rpcServer.getClientProtocolModule().
        logAuditEvent(true, OperationName.DISABLE_ERASURE_CODING_POLICY,
            this.rpcServer.getClientProtocolModule().INVOKE_TYPE_CONCURRENT);
  }

  public void enableErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    rpcServer.checkOperation(OperationCategory.WRITE);

    RemoteMethod method = new RemoteMethod("enableErasureCodingPolicy",
        new Class<?>[] {String.class}, ecPolicyName);
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    try {
      rpcClient.invokeConcurrent(nss, method, true, false);
    } catch (IOException e) {
      this.rpcServer.getClientProtocolModule().
          logAuditEvent(false, OperationName.ENABLE_ERASURE_CODING_POLICY,
              this.rpcServer.getClientProtocolModule().INVOKE_TYPE_CONCURRENT);
      throw e;
    }
    this.rpcServer.getClientProtocolModule().
        logAuditEvent(true, OperationName.ENABLE_ERASURE_CODING_POLICY,
            this.rpcServer.getClientProtocolModule().INVOKE_TYPE_CONCURRENT);
  }

  public ErasureCodingPolicy getErasureCodingPolicy(String src)
      throws IOException {
    rpcServer.checkOperation(OperationCategory.READ);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false, false);
    RemoteMethod remoteMethod = new RemoteMethod("getErasureCodingPolicy",
        new Class<?>[] {String.class}, new RemoteParam());
    ErasureCodingPolicy ret;
    try {
      ret = rpcClient.invokeSequential(
          locations, remoteMethod, null, null);
    } catch (IOException e) {
      this.rpcServer.getClientProtocolModule().
          logAuditEvent(false, OperationName.GET_ERASURE_CODING_POLICY,
              this.rpcServer.getClientProtocolModule().INVOKE_TYPE_CONCURRENT, src);
      throw e;
    }
    this.rpcServer.getClientProtocolModule().
        logAuditEvent(true, OperationName.GET_ERASURE_CODING_POLICY,
            this.rpcServer.getClientProtocolModule().INVOKE_TYPE_CONCURRENT, src);
    return ret;
  }

  public void setErasureCodingPolicy(String src, String ecPolicyName)
      throws IOException {
    rpcServer.checkOperation(OperationCategory.WRITE);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false, false);
    RemoteMethod remoteMethod = new RemoteMethod("setErasureCodingPolicy",
        new Class<?>[] {String.class, String.class},
        new RemoteParam(), ecPolicyName);
    String invokeType = this.rpcServer.getClientProtocolModule().INVOKE_TYPE_SEQUENTIAL;
    try {
      boolean isFixedOrder = rpcServer.isFixedOrder(src);
      if (rpcServer.isInvokeConcurrent(src) || isFixedOrder) {
        invokeType = this.rpcServer.getClientProtocolModule().INVOKE_TYPE_CONCURRENT;
        rpcClient.invokeConcurrent(locations, remoteMethod, isFixedOrder);
      } else {
        rpcClient.invokeSequential(locations, remoteMethod);
      }
    } catch (IOException e) {
      this.rpcServer.getClientProtocolModule().
          logAuditEvent(false, OperationName.SET_ERASURE_CODING_POLICY, invokeType, src);
      throw e;
    }
    this.rpcServer.getClientProtocolModule().
        logAuditEvent(true, OperationName.SET_ERASURE_CODING_POLICY, invokeType, src);
  }

  public void unsetErasureCodingPolicy(String src) throws IOException {
    rpcServer.checkOperation(OperationCategory.WRITE);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false, false);
    RemoteMethod remoteMethod = new RemoteMethod("unsetErasureCodingPolicy",
        new Class<?>[] {String.class}, new RemoteParam());
    String invokeType = this.rpcServer.getClientProtocolModule().INVOKE_TYPE_SEQUENTIAL;
    try {
      boolean isFixedOrder = rpcServer.isFixedOrder(src);
      if (rpcServer.isInvokeConcurrent(src) || isFixedOrder) {
        invokeType = this.rpcServer.getClientProtocolModule().INVOKE_TYPE_CONCURRENT;
        rpcClient.invokeConcurrent(locations, remoteMethod, isFixedOrder);
      } else {
        rpcClient.invokeSequential(locations, remoteMethod);
      }
    } catch (IOException e) {
      this.rpcServer.getClientProtocolModule().
          logAuditEvent(false, OperationName.UNSET_ERASURE_CODING_POLICY, invokeType, src);
      throw e;
    }
    this.rpcServer.getClientProtocolModule().
        logAuditEvent(true, OperationName.UNSET_ERASURE_CODING_POLICY, invokeType, src);
  }

  public ECTopologyVerifierResult getECTopologyResultForPolicies(
      String[] policyNames) throws IOException {
    RemoteMethod method = new RemoteMethod("getECTopologyResultForPolicies",
        new Class<?>[] {String[].class}, new Object[] {policyNames});
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    if (nss.isEmpty()) {
      throw new IOException("No namespace availaible.");
    }
    Map<FederationNamespaceInfo, ECTopologyVerifierResult> ret = rpcClient
        .invokeConcurrent(nss, method, true, false,
            ECTopologyVerifierResult.class);
    for (Map.Entry<FederationNamespaceInfo, ECTopologyVerifierResult> entry : ret
        .entrySet()) {
      if (!entry.getValue().isSupported()) {
        return entry.getValue();
      }
    }
    // If no negative result, return the result from the first namespace.
    return ret.get(nss.iterator().next());
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
