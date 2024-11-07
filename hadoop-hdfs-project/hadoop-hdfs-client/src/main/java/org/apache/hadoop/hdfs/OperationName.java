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
  public static String ABANDON_BLOCK = "abandonBlock";
  public static String ADD_ERASURE_CODING_POLICIES = "addErasureCodingPolicies";
  public static String ADD_CACHE_DIRECTIVE = "addCacheDirective";
  public static String ADD_CACHE_POOL = "addCachePool";
  public static String ALLOW_SNAPSHOT = "allowSnapshot";
  public static String APPEND = "append";
  public static String BUMP_BLOCK_GENERATION_STAMP = "bumpBlockGenerationStamp";
  public static String CANCEL_DELEGATION_TOKEN = "cancelDelegationToken";
  public static String CHECK_ACCESS = "checkAccess";
  public static String CLEAR_CORRUPT_LAZY_PERSIST_FILES = "clearCorruptLazyPersistFiles";
  public static String COMPLETE = "complete";
  public static String COMPLETE_FILE = "completeFile";
  public static String COMPUTE_SNAPSHOT_DIFF = "computeSnapshotDiff";
  public static String COMMIT_BLOCK_SYNCHRONIZATION = "commitBlockSynchronization";
  public static String CONCAT = "concat";
  public static String CONTENT_SUMMARY = "contentSummary";
  public static String CREATE = "create";
  public static String CREATE_ENCRYPTION_ZONE = "createEncryptionZone";
  public static String CREATE_SNAPSHOT = "createSnapshot";
  public static String CREATE_SYMLINK = "createSymlink";
  public static String DATANODE_REPORT = "datanodeReport";
  public static String DELETE = "delete";
  public static String DELETE_SNAPSHOT = "deleteSnapshot";
  public static String DISABLE_ERASURE_CODING_POLICY = "disableErasureCodingPolicy";
  public static String DISALLOW_SNAPSHOT = "disallowSnapshot";
  public static String ENTER_SAFE_MODE = "enterSafeMode";
  public static String ENABLE_ERASURE_CODING_POLICY = "enableErasureCodingPolicy";
  public static String END_CHECKPOINT = "endCheckpoint";
  public static String FSCK = "fsck";
  public static String FSCK_GET_BLOCK_LOCATIONS = "fsckGetBlockLocations";
  public static String FS_TREE_TRAVERSER = "FSTreeTraverser";
  public static String FINALIZE_UPGRADE = "finalizeUpgrade";
  public static String FINALIZE_ROLLING_UPGRADE = "finalizeRollingUpgrade";
  public static String FSYNC = "fsync";
  public static String GET_ACL_STATUS = "getAclStatus";
  public static String GET_ADDITIONAL_BLOCK = "getAdditionalBlock";
  public static String GET_ADDITIONAL_DATANODE = "getAdditionalDatanode";
  public static String GET_BLOCKS = "getBlocks";
  public static String GET_COMPLETE_BLOCKS_TOTAL = "getCompleteBlocksTotal";
  public static String GET_CONTENT_SUMMARY = "getContentSummary";
  public static String GET_DATANODE_REPORT = "getDatanodeReport";
  public static String GET_DATANODE_STORAGE_REPORT = "getDatanodeStorageReport";
  public static String GET_DELEGATION_TOKEN = "getDelegationToken";
  public static String GET_LOCATED_FILE_INFO = "getLocatedFileInfo";
  public static String GET_EC_TOPOLOGY_RESULT_FOR_POLICIES = "getECTopologyResultForPolicies";
  public static String GET_ERASURE_CODING_CODECS = "getErasureCodingCodecs";
  public static String GET_ERASURE_CODING_POLICIES = "getErasureCodingPolicies";
  public static String GET_ERASURE_CODING_POLICY = "getErasureCodingPolicy";
  public static String GET_EZ_FOR_PATH = "getEZForPath";
  public static String GET_FILE_INFO = "getFileInfo";
  public static String GET_FILE_LINK_INFO = "getFileLinkInfo";
  public static String GET_KEY_NAME_FOR_ZONE = "getKeyNameForZone";
  public static String GET_LISTING = "getListing";
  public static String GET_NAMESPACE_INFO = "getNamespaceInfo";
  public static String GET_NUMBER_OF_DATANODES = "getNumberOfDatanodes";
  public static String GET_QUOTA_USAGE = "getQuotaUsage";
  public static String GET_PREFERRED_BLOCK_SIZE = "getPreferredBlockSize";
  public static String GET_ROLLING_UPGRADE_STATUS = "getRollingUpgradeStatus";
  public static String GET_STORAGE_POLICY = "getStoragePolicy";
  public static String GET_STORAGE_POLICIES = "getStoragePolicies";
  public static String GET_TOPOLOGY_REPORT = "topologyReport";
  public static String GET_XATTRS = "getXAttrs";
  public static String GET_ZONE_STATUS = "getZoneStatus";
  public static String HANDLE_HEARTBEAT = "handleHeartbeat";
  public static String IS_FILE_CLOSED = "isFileClosed";
  public static String LEAVE_SAFE_MODE = "leaveSafeMode";
  public static String LIST_CACHE_DIRECTIVES = "listCacheDirectives";
  public static String LIST_CACHE_POOLS = "listCachePools";
  public static String LIST_CORRUPT_FILE_BLOCKS = "listCorruptFileBlocks";
  public static String LIST_HIGH_RISK_BLOCKS = "listHighRiskBlocks";
  public static String LIST_ENCRYPTION_ZONES = "listEncryptionZones";
  public static String LIST_OPEN_FILES = "listOpenFiles";
  public static String LIST_XATTRS = "listXAttrs";
  public static String LIST_REENCRYPTION_STATUS = "listReencryptionStatus";
  public static String LIST_SNAPSHOT_TABLE_DIRECTORY = "listSnapshottableDirectory";
  public static String LIST_STATUS = "listStatus";
  public static String LOAD_FSIMAGE = "loadFSImage";
  public static String META_SAVE = "metaSave";
  public static String MKDIRS = "mkdirs";
  public static String MODIFY_ACL_ENTRIES = "modifyAclEntries";
  public static String MODIFY_CACHE_DIRECTIVE = "modifyCacheDirective";
  public static String MODIFY_CACHE_POOL = "modifyCachePool";
  public static String OPEN = "open";
  public static String PAUSE_FOR_TESTING_AFTER_NTH_CHECKPOINT = "pauseForTestingAfterNthCheckpoint";
  public static String PROCESS_INCREMENTAL_BLOCK_REPORT = "processIncrementalBlockReport";
  public static String PROCESS_MISREPLICATES_ASYNC = "processMisReplicatesAsync";
  public static String PROCESS_MAINTENANCE_NODES = "processMaintenanceNodes";
  public static String PROCESS_REPORT = "processReport";
  public static String PROCESS_CACHE_REPORT = "processCacheReport";
  public static String PROCESS_PENDING_RECONSTRUCTIONS = "processPendingReconstructions";
  public static String QUOTA_USAGE = "quotaUsage";
  public static String QUERY_ROLLING_UPGRADE = "queryRollingUpgrade";
  public static String RECOVER_LEASE = "recoverLease";
  public static String REENCRYPTION_HANDLER = "reencryptionHandler";
  public static String REENCRYPTION_UPDATER = "reencryptUpdater";
  public static String REFRESH_NODES = "refreshNodes";
  public static String REFRESH_TOPOLOGY = "refreshTopology";
  public static String REGISTER_DATANODE = "registerDatanode";
  public static String REGISTER_BACKUP_NODE = "registerBackupNode";
  public static String RELEASE_BACKUP_NODE = "releaseBackupNode";
  public static String REMOVE_ACL = "removeAcl";
  public static String REMOVE_ACL_ENTRIES = "removeAclEntries";
  public static String REMOVE_BLOCKS = "removeBlocks";
  public static String REMOVE_BR_LEASE_IF_NEEDED = "removeBRLeaseIfNeeded";
  public static String REMOVE_CACHE_DIRECTIVE = "removeCacheDirective";
  public static String REMOVE_CACHE_POOL = "removeCachePool";
  public static String CACHE_REPLICATION_MONITOR_RESCAN = "cacheReplicationMonitorRescan";
  public static String REMOVE_DEFAULT_ACL = "removeDefaultAcl";
  public static String REMOVE_DATANODE = "removeDatanode";
  public static String REMOVE_ERASURE_CODING_POLICY = "removeErasureCodingPolicy";
  public static String REMOVE_XATTRS = "removeXAttr";
  public static String RENAME = "rename";
  public static String RENAME2 = "rename2";
  public static String RENAME_SNAPSHOT = "renameSnapshot";
  public static String RENEW_DELEGATION_TOKEN = "renewDelegationToken";
  public static String RENEW_LEASE = "renewLease";
  public static String REPORT_BAD_BLOCKS = "reportBadBlocks";
  public static String ROLL_EDITS = "rollEdits";
  public static String ROLL_EDIT_LOG = "rollEditLog";
  public static String SATISFY_STORAGE_POLICY = "satisfyStoragePolicy";
  public static String SAVE_NAMESPACE = "saveNamespace";
  public static String SCAN_AND_COMPACT_STORAGES = "scanAndCompactStorages";
  public static String SCANNER_MISREPLICATES_ASYNC = "scannerMisReplicatesAsync";
  public static String SET_ACL = "setAcl";
  public static String SET_BALANCER_BANDWIDTH = "setBalancerBandwidth";
  public static String SET_ERASURE_CODING_POLICY = "setErasureCodingPolicy";
  public static String SET_IMAGE_LOADED = "setImageLoaded";
  public static String SET_OWNER = "setOwner";
  public static String SET_PERMISSION = "setPermission";
  public static String SET_QUOTA = "setQuota";
  public static String SET_REPLICATION = "setReplication";
  public static String SET_SAFE_MODE = "setSafeMode";
  public static String SET_STORAGE_POLICY = "setStoragePolicy";
  public static String SET_TIMES = "setTimes";
  public static String SET_XATTR = "setXAttr";
  public static String START_ACTIVE_SERVICE = "startActiveServices";
  public static String START_CHECKPOINT = "startCheckpoint";
  public static String START_COMMON_SERVICE = "startCommonServices";
  public static String START_ROLLING_UPGRADE = "startRollingUpgrade";
  public static String STOP_ACTIVE_SERVICE = "stopActiveServices";
  public static String STOP_COMMON_SERVICE = "stopCommonServices";
  public static String STOP_REENCRYPT_THREAD = "stopReencryptThread";
  public static String TRUNCATE = "truncate";
  public static String UPDATE_PIPELINE = "updatePipeline";
  public static String UPDATE_NEEDED_RECONSTRUCTIONS = "updateNeededReconstructions";
  public static String UNSET_ERASURE_CODING_POLICY = "unsetErasureCodingPolicy";
  public static String UNSET_STORAGE_POLICY = "unsetStoragePolicy";
  public static String PROCESS_TIME_OUT_EXCESS_BLOCKS = "processTimeOutExcessBlocks";
  public static String COMPUTE_BLOCK_RECONSTRUCTION_WORK = "computeBlockReconstructionWork";
  public static String COMPUTE_BLOCK_RECONSTRUCTION_WORK_FOR_BLOCKS = "computeReconstructionWorkForBlocks";
  public static String COMPUTE_DATANODE_WORK = "computeDatanodeWork";
  public static String RESCAN_POSTPONED_MISREPLICATED_BLOCKS = "rescanPostponedMisreplicatedBlocks";
  public static String INVALIDATE_WORK_FOR_ONE_NODE = "invalidateWorkForOneNode";
}
