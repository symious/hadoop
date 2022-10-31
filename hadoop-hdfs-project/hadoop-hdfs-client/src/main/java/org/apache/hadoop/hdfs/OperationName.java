/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 *     http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

public class OperationName {
  public static String ADD_ERASURE_CODING_POLICIES = "addErasureCodingPolicies";
  public static String APPEND = "append";
  public static String CANCEL_DELEGATION_TOKEN = "cancelDelegationToken";
  public static String CHECK_ACCESS = "checkAccess";
  public static String COMPLETE = "complete";
  public static String CONCAT = "concat";
  public static String CREATE = "create";
  public static String DELETE = "delete";
  public static String DISABLE_ERASURE_CODING_POLICY = "disableErasureCodingPolicy";
  public static String ENABLE_ERASURE_CODING_POLICY = "enableErasureCodingPolicy";
  public static String GET_ACL_STATUS = "getAclStatus";
  public static String GET_CONTENT_SUMMARY = "getContentSummary";
  public static String GET_DATANODE_REPORT = "getDatanodeReport";
  public static String GET_DATANODE_STORAGE_REPORT = "getDatanodeStorageReport";
  public static String GET_LOCATED_FILE_INFO = "getLocatedFileInfo";
  public static String GET_ERASURE_CODING_CODECS = "getErasureCodingCodecs";
  public static String GET_ERASURE_CODING_POLICIES = "getErasureCodingPolicies";
  public static String GET_ERASURE_CODING_POLICY = "getErasureCodingPolicy";
  public static String GET_FILE_INFO = "getFileInfo";
  public static String GET_FILE_LINK_INFO = "getFileLinkInfo";
  public static String GET_LISTING = "getListing";
  public static String GET_QUOTA_USAGE = "getQuotaUsage";
  public static String GET_STORAGE_POLICY = "getStoragePolicy";
  public static String GET_XATTRS = "getXAttrs";
  public static String IS_FILE_CLOSED = "isFileClosed";
  public static String LIST_XATTRS = "listXAttrs";
  public static String META_SAVE = "metaSave";
  public static String MKDIRS = "mkdirs";
  public static String MODIFY_ACL_ENTRIES = "modifyAclEntries";
  public static String OPEN = "open";
  public static String RECOVER_LEASE = "recoverLease";
  public static String REFRESH_NODES = "refreshNodes";
  public static String REFRESH_TOPOLOGY = "refreshTopology";
  public static String REMOVE_ACL = "removeAcl";
  public static String REMOVE_ACL_ENTRIES = "removeAclEntries";
  public static String REMOVE_DEFAULT_ACL = "removeDefaultAcl";
  public static String REMOVE_ERASURE_CODING_POLICY = "removeErasureCodingPolicy";
  public static String REMOVE_XATTRS = "removeXAttr";
  public static String RENAME = "rename";
  public static String RENAME2 = "rename2";
  public static String RENEW_DELEGATION_TOKEN = "renewDelegationToken";
  public static String RENEW_LEASE = "renewLease";
  public static String ROLL_EDITS = "rollEdits";
  public static String SATISFY_STORAGE_POLICY = "satisfyStoragePolicy";
  public static String SAVE_NAMESPACE = "saveNamespace";
  public static String SET_ACL = "setAcl";
  public static String SET_BALANCER_BANDWIDTH = "setBalancerBandwidth";
  public static String SET_ERASURE_CODING_POLICY = "setErasureCodingPolicy";
  public static String SET_OWNER = "setOwner";
  public static String SET_PERMISSION = "setPermission";
  public static String SET_QUOTA = "setQuota";
  public static String SET_REPLICATION = "setReplication";
  public static String SET_SAFE_MODE = "setSafeMode";
  public static String SET_STORAGE_POLICY = "setStoragePolicy";
  public static String SET_TIMES = "setTimes";
  public static String SET_XATTR = "setXAttr";
  public static String TRUNCATE = "truncate";
  public static String UNSET_ERASURE_CODING_POLICY = "unsetErasureCodingPolicy";
  public static String UNSET_STORAGE_POLICY = "unsetStoragePolicy";
}
