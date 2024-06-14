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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSClientAdapter;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.MultipleDestinationMountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.order.DestinationOrder;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

import static org.apache.hadoop.test.Whitebox.setInternalState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;

public class TestRouterRpcFixedOrder {

  private final static int NUM_SUBCLUSTERS = 3;
  private static StateStoreDFSCluster cluster;
  private static MiniRouterDFSCluster.RouterContext routerContext;
  private static DFSClient routerClient;
  private static MiniRouterDFSCluster.NamenodeContext nnContextMain;
  private static MiniRouterDFSCluster.NamenodeContext nnContextSub0;
  private static MiniRouterDFSCluster.NamenodeContext nnContextSub1;
  private static FileSystem nnFsMain;
  private static FileSystem nnFsSub0;
  private static FileSystem nnFsSub1;
  private static RouterClientProtocol spyClientProtocol;

  @BeforeClass
  public static void setup() throws Exception {
    cluster = new StateStoreDFSCluster(false, NUM_SUBCLUSTERS,
        MultipleDestinationMountTableResolver.class);
    Configuration conf = new RouterConfigBuilder().stateStore().heartbeat().admin().rpc().build();
    conf.set(RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE, "ns0,ns1,ns2");
    conf.setInt(CommonConfigurationKeys.FS_TRASH_INTERVAL_KEY, (int) 1E9);
    conf.setStrings(CommonConfigurationKeys.FS_TRASH_ROOT, "/Trash");
    cluster.addRouterOverrides(conf);
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp(true);
    routerContext = cluster.getRandomRouter();
    routerContext.getRouter().getStateStore().stopCacheUpdateService();
    routerClient = routerContext.getClient();
    spyClientProtocol = spy(routerContext.getRouterRpcServer().getClientProtocolModule());
    setInternalState(routerContext.getRouterRpcServer(), "clientProto", spyClientProtocol);
    nnContextMain = cluster.getNamenode("ns2", null);
    nnContextSub0 = cluster.getNamenode("ns0", null);
    nnContextSub1 = cluster.getNamenode("ns1", null);
    nnFsMain = nnContextMain.getFileSystem();
    nnFsSub0 = nnContextSub0.getFileSystem();
    nnFsSub1 = nnContextSub1.getFileSystem();

    MultipleDestinationMountTableResolver mountTable =
        (MultipleDestinationMountTableResolver) routerContext.getRouter().getSubclusterResolver();
    Map<String, String> mapFixed = new LinkedHashMap<>();
    mapFixed.put("ns2", "/");
    mapFixed.put("ns0", "/");
    mapFixed.put("ns1", "/");
    MountTable fixedEntry = MountTable.newInstance("/", mapFixed);
    fixedEntry.setDestOrder(DestinationOrder.FIXED);
    mountTable.addEntry(fixedEntry);
    // Add an entry for /Trash
    Map<String, String> mapTrash = new LinkedHashMap<>();
    mapTrash.put("ns2", "/Trash");
    mapTrash.put("ns0", "/Trash");
    mapTrash.put("ns1", "/Trash");
    MountTable trashEntry = MountTable.newInstance("/Trash", mapTrash);
    trashEntry.setDestOrder(DestinationOrder.HASH_ALL);
    mountTable.addEntry(trashEntry);
  }

  @AfterClass
  public static void cleanup() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testMkdirs() throws IOException {
    routerClient.mkdirs("/testMkdirs");
    Path path = new Path("/testMkdirs");
    // Should appear on all 3 NNs
    assertTrue(nnFsMain.exists(path));
    assertTrue(nnFsSub0.exists(path));
    assertTrue(nnFsSub1.exists(path));
  }

  @Test
  public void testCreate() throws IOException {
    // New create, will create a new file and ancestor dir tree only on main ns
    routerClient.create("/testCreate/file1", true).close();
    Path dirPath = new Path("/testCreate");
    Path path1 = new Path("/testCreate/file1");
    assertTrue(nnFsMain.exists(dirPath));
    assertFalse(nnFsSub0.exists(dirPath));
    assertFalse(nnFsSub1.exists(dirPath));
    assertTrue(nnFsMain.exists(path1));
    assertFalse(nnFsSub0.exists(path1));
    assertFalse(nnFsSub1.exists(path1));
    // Create on existing file on sub ns
    // Will not create a new file on any other ns
    Path path2 = new Path("/testCreate/file2");
    nnFsSub0.create(path2);
    OutputStream os = routerClient.create(path2.toString(), true);
    os.write(42);
    os.close();
    assertFalse(nnFsMain.exists(path2));
    assertTrue(nnFsSub0.exists(path2));
    assertFalse(nnFsSub1.exists(path2));
    FSDataInputStream is = nnFsSub0.open(path2);
    assertEquals(42, is.read());
    is.close();
  }

  @Test
  public void testGetInfo() throws IOException {
    // Should succeed even if files only exist on sub ns
    nnFsSub1.mkdirs(new Path("/testGetInfo"));
    Path path = new Path("/testGetInfo/file");
    nnFsSub1.create(path).close();
    assertFalse(nnFsMain.exists(path));
    assertFalse(nnFsSub0.exists(path));
    assertTrue(nnFsSub1.exists(path));
    assertNotNull(routerClient.getFileInfo(path.toString()));

    // When files are scattered across nss, prioritize main
    path = new Path("/testGetContentSummary");
    nnFsMain.mkdirs(path);
    nnFsSub0.mkdirs(path);
    nnFsSub1.mkdirs(path);
    nnFsMain.create(new Path(path, "file1"), (short) 3).close();
    nnFsSub0.create(new Path(path, "file1"), (short) 2).close();
    nnFsSub1.create(new Path(path, "file2"), (short) 3).close();
    DirectoryListing listing =
        routerClient.listPaths(path.toString(), HdfsFileStatus.EMPTY_NAME, false);
    // Both files should have 3 repl
    assertEquals(3, listing.getPartialListing()[0].getReplication());
    assertEquals(3, listing.getPartialListing()[1].getReplication());
  }

  @Test
  public void testSetStoragePolicy() throws IOException {
    routerClient.mkdirs("/testSetStoragePolicy/1");
    routerClient.mkdirs("/testSetStoragePolicy/2");
    nnFsSub0.create(new Path("/testSetStoragePolicy/2/1"), (short) 3).close();
    nnFsSub1.create(new Path("/testSetStoragePolicy/2/1"), (short) 3).close();
    // Proxy to all ns
    routerClient.setStoragePolicy("/testSetStoragePolicy/1",
        HdfsConstants.COLD_STORAGE_POLICY_NAME);
    assertEquals(HdfsConstants.COLD_STORAGE_POLICY_NAME,
        nnFsMain.getStoragePolicy(new Path("/testSetStoragePolicy/1")).getName());
    assertEquals(HdfsConstants.COLD_STORAGE_POLICY_NAME,
        nnFsSub0.getStoragePolicy(new Path("/testSetStoragePolicy/1")).getName());
    assertEquals(HdfsConstants.COLD_STORAGE_POLICY_NAME,
        nnFsSub1.getStoragePolicy(new Path("/testSetStoragePolicy/1")).getName());
    routerClient.setStoragePolicy("/testSetStoragePolicy/2/1",
        HdfsConstants.WARM_STORAGE_POLICY_NAME);
    assertEquals(HdfsConstants.WARM_STORAGE_POLICY_NAME,
        nnFsSub0.getStoragePolicy(new Path("/testSetStoragePolicy/2/1")).getName());
    assertEquals(HdfsConstants.WARM_STORAGE_POLICY_NAME,
        nnFsSub1.getStoragePolicy(new Path("/testSetStoragePolicy/2/1")).getName());
  }

  @Test
  public void testDelete() throws Exception {
    routerClient.mkdirs("/Trash");
    routerClient.mkdirs("/testDelete");

    nnFsSub0.create(new Path("/testDelete/file")).close();
    nnFsSub1.create(new Path("/testDelete/file")).close();
    nnFsMain.mkdirs(new Path("/testDelete/dir"));
    nnFsSub0.mkdirs(new Path("/testDelete/dir"));
    nnFsSub1.mkdirs(new Path("/testDelete/dir"));
    assertTrue(nnFsSub0.exists(new Path("/testDelete/file")));
    assertTrue(nnFsSub1.exists(new Path("/testDelete/file")));
    assertTrue(nnFsMain.exists(new Path("/testDelete/dir")));
    assertTrue(nnFsSub0.exists(new Path("/testDelete/dir")));
    assertTrue(nnFsSub1.exists(new Path("/testDelete/dir")));

    routerClient.delete("/testDelete/file");
    routerClient.delete("/testDelete/dir");
    // Should delete everything without any exceptions
    assertFalse(nnFsSub0.exists(new Path("/testDelete/file")));
    assertFalse(nnFsSub1.exists(new Path("/testDelete/file")));
    assertFalse(nnFsMain.exists(new Path("/testDelete/dir")));
    assertFalse(nnFsSub0.exists(new Path("/testDelete/dir")));
    assertFalse(nnFsSub1.exists(new Path("/testDelete/dir")));
  }

  @Test
  public void testThrow() throws Exception {
    routerClient.mkdirs("/testThrow");
    UserGroupInformation bob =
        UserGroupInformation.createUserForTesting("bob", new String[] { "group" });
    DFSClient bobClient = DFSClientAdapter.getDFSClient(
        (DistributedFileSystem) DFSTestUtil.getFileSystemAs(bob, routerContext.getConf()));

    nnFsSub0.mkdirs(new Path("/testThrow/1/a/b/c/d"));
    nnFsSub1.mkdirs(new Path("/testThrow/1/a/e/f"));
    nnFsSub1.setPermission(new Path("/testThrow/1/a/e/"),
        FsPermission.createImmutable((short) 0777));
    // Should set policy successfully for nnFsSub1. FNF on the other 2 ns should be ignored.
    bobClient.setStoragePolicy("/testThrow/1/a/e", HdfsConstants.COLD_STORAGE_POLICY_NAME);
    assertEquals(HdfsConstants.COLD_STORAGE_POLICY_NAME,
        nnFsSub1.getStoragePolicy(new Path("/testThrow/1/a/e")).getName());

    nnFsMain.mkdirs(new Path("/testThrow/2/a/b/c/d"));
    nnFsSub0.mkdirs(new Path("/testThrow/2/a/e/f"));
    nnFsSub1.mkdirs(new Path("/testThrow/2/a/e/f"), FsPermission.createImmutable((short) 0111));
    // SetStoragePolicy should fail on the readonly dir
    assertThrows(AccessControlException.class, () -> bobClient.setStoragePolicy("/testThrow/2/a/e",
        HdfsConstants.COLD_STORAGE_POLICY_NAME));
  }

  @Test
  public void testRename() throws Exception {
    nnFsMain.delete(new Path("/testRename"), true);
    nnFsSub0.delete(new Path("/testRename"), true);
    nnFsSub1.delete(new Path("/testRename"), true);
    MultipleDestinationMountTableResolver mountTable =
        (MultipleDestinationMountTableResolver) routerContext.getRouter().getSubclusterResolver();
    Map<String, String> mapFixed = new LinkedHashMap<>();
    mapFixed.put("ns2", "/testRename");
    mapFixed.put("ns0", "/testRename");
    mapFixed.put("ns1", "/testRename");
    MountTable fixedEntry = MountTable.newInstance("/mnt", mapFixed);
    fixedEntry.setDestOrder(DestinationOrder.FIXED);
    mountTable.addEntry(fixedEntry);

    // Failure cases
    // src does not exist
    setupRename(nnFsSub0, "dst", true, false);
    assertThrowsWithMessage(FileNotFoundException.class,
        "Source /mnt/src/src does not exist on any namespace.",
        () -> routerClient.rename("/mnt/src/src", "/mnt/dst/dst"));
    cleanupRename();
    // dst parent does not exist
    setupRename(nnFsSub0, "src", true, false);
    assertThrowsWithMessage(FileNotFoundException.class,
        "Destination parent /mnt/dst does not exist on any namespace.",
        () -> routerClient.rename("/mnt/src/src", "/mnt/dst/dst"));
    cleanupRename();
    // dst is an existing file, regardless of same ns as src or not
    setupRename(nnFsSub0, "src", true, false);
    setupRename(nnFsSub1, "src.file", true, true);
    setupRename(nnFsMain, "dst.file", false, true);
    assertThrowsWithMessage(IOException.class,
        "Cannot rename a directory /mnt/src/src to a file /mnt/dst/dst.file",
        () -> routerClient.rename("/mnt/src/src", "/mnt/dst/dst.file"));
    assertThrowsWithMessage(IOException.class,
        "Destination already exists: /mnt/dst/dst.file",
        () -> routerClient.rename("/mnt/src/src.file", "/mnt/dst/dst.file"));
    cleanupRename();
    // Inconsistent src being file on one ns and dir on another
    setupRename(nnFsSub0, "src", true, true);
    setupRename(nnFsSub1, "src", true, false);
    setupRename(nnFsSub0, "dst0", false, false);
    setupRename(nnFsSub1, "dst1", false, false);
    assertThrowsWithMessage(IOException.class,
        "Inconsistent state with mixed files/dirs in /mnt/src/src",
        () -> routerClient.rename("/mnt/src/src", "/mnt/dst/dst0"));
    assertThrowsWithMessage(IOException.class,
        "Inconsistent state with mixed files/dirs in /mnt/src/src",
        () -> routerClient.rename("/mnt/src/src", "/mnt/dst/dst1"));
    cleanupRename();
    // Inconsistent src being multiple files on multiple namespaces
    setupRename(nnFsSub0, "src.file", true, true);
    setupRename(nnFsSub1, "src.file", true, true);
    setupRename(nnFsSub0, "dst", false, false);
    assertThrowsWithMessage(IOException.class,
        "Inconsistent state with the same file /mnt/src/src.file detected on multiple namespaces",
        () -> routerClient.rename("/mnt/src/src.file", "/mnt/dst/dst"));
    cleanupRename();
    // dst already contains a dir/file with the same name as src
    setupRename(nnFsSub0, "src", true, true);
    setupRename(nnFsSub1, "src.file", true, false);
    setupRename(nnFsMain, "dst0", false, false);
    nnFsMain.mkdirs(new Path("/testRename/dst/dst0/src"));
    setupRename(nnFsMain, "dst1", false, false);
    nnFsMain.create(new Path("/testRename/dst/dst1/src.file")).close();
    assertThrowsWithMessage(IOException.class,
        "Destination already exists: /mnt/dst/dst0/src",
        () -> routerClient.rename("/mnt/src/src", "/mnt/dst/dst0"));
    assertThrowsWithMessage(IOException.class,
        "Destination already exists: /mnt/dst/dst1/src.file",
        () -> routerClient.rename("/mnt/src/src.file", "/mnt/dst/dst1"));
    cleanupRename();

    // All cases below should not fail
    // 2 cases for src being a file
    setupRename(nnFsSub0, "src.file", true, true);
    setupRename(nnFsSub1, "", false, false);
    routerClient.rename("/mnt/src/src.file", "/mnt/dst/dst.file");
    assertTrue(nnFsSub0.getFileStatus(new Path("/testRename/dst/dst.file")).isFile());
    cleanupRename();
    setupRename(nnFsSub0, "src.file", true, true);
    setupRename(nnFsSub1, "dst", false, false);

    routerClient.rename("/mnt/src/src.file", "/mnt/dst/dst");
    assertTrue(nnFsSub0.exists(new Path("/testRename/dst/dst/src.file")));
    cleanupRename();

    // ==============
    // Single dir src
    // dst doesn't exist, parent exists, doesn't matter which ns
    setupRename(nnFsSub0, "src", true, false);
    setupRename(nnFsMain, "", false, false);

    routerClient.rename("/mnt/src/src", "/mnt/dst/dst");
    assertTrue(nnFsSub0.exists(new Path("/testRename/dst/dst/file")));
    assertFalse(nnFsMain.exists(new Path("/testRename/dst/dst")));
    cleanupRename();

    // dst dir exists on same ns and on different ns
    setupRename(nnFsSub0, "src1", true, false);
    setupRename(nnFsSub0, "src2", true, false);
    setupRename(nnFsSub0, "dst1", false, false);
    setupRename(nnFsSub1, "dst2", false, false);

    routerClient.rename("/mnt/src/src1", "/mnt/dst/dst1");
    routerClient.rename("/mnt/src/src2", "/mnt/dst/dst2");
    assertTrue(nnFsSub0.exists(new Path("/testRename/dst/dst1/src1/file")));
    assertFalse(nnFsSub1.exists(new Path("/testRename/dst/dst1")));
    assertTrue(nnFsSub0.exists(new Path("/testRename/dst/dst2/src2/file")));
    assertTrue(nnFsSub1.exists(new Path("/testRename/dst/dst2")));
    cleanupRename();

    // ================
    // Multiple dir src
    // dst doesn't exist, parent exists, doesn't matter which ns, doesn't matter how many parent
    setupRename(nnFsSub0, "src", true, false);
    setupRename(nnFsSub1, "src", true, false);
    setupRename(nnFsMain, "", false, false);

    routerClient.rename("/mnt/src/src", "/mnt/dst/dst");
    assertTrue(nnFsSub0.exists(new Path("/testRename/dst/dst/file")));
    assertTrue(nnFsSub1.exists(new Path("/testRename/dst/dst/file")));
    assertFalse(nnFsMain.exists(new Path("/testRename/dst/dst")));
    cleanupRename();

    // dst exists
    // same ns
    setupRename(nnFsSub0, "src1", true, false);
    setupRename(nnFsSub1, "src1", true, false);
    setupRename(nnFsSub1, "dst1", false, false);

    routerClient.rename("/mnt/src/src1", "/mnt/dst/dst1");
    assertTrue(nnFsSub0.exists(new Path("/testRename/dst/dst1/src1/file")));
    assertTrue(nnFsSub1.exists(new Path("/testRename/dst/dst1/src1/file")));
    // different ns
    setupRename(nnFsSub0, "src2", true, false);
    setupRename(nnFsSub1, "src2", true, false);
    setupRename(nnFsMain, "dst2", false, false);

    routerClient.rename("/mnt/src/src2", "/mnt/dst/dst2");
    assertTrue(nnFsSub0.exists(new Path("/testRename/dst/dst2/src2/file")));
    assertTrue(nnFsSub1.exists(new Path("/testRename/dst/dst2/src2/file")));
    assertFalse(nnFsMain.exists(new Path("/testRename/dst/dst2/src2")));
    // mixed ns
    setupRename(nnFsSub0, "src3", true, false);
    setupRename(nnFsSub1, "src3", true, false);
    setupRename(nnFsSub1, "dst3", false, false);
    setupRename(nnFsMain, "dst3", false, false);

    routerClient.rename("/mnt/src/src3", "/mnt/dst/dst3");
    assertTrue(nnFsSub0.exists(new Path("/testRename/dst/dst3/src3/file")));
    assertTrue(nnFsSub1.exists(new Path("/testRename/dst/dst3/src3/file")));
    assertFalse(nnFsMain.exists(new Path("/testRename/dst/dst3/src3/file")));
    cleanupRename();
  }

  @Test
  public void testRename2() throws Exception {
    nnFsMain.delete(new Path("/testRename"), true);
    nnFsSub0.delete(new Path("/testRename"), true);
    nnFsSub1.delete(new Path("/testRename"), true);
    MultipleDestinationMountTableResolver mountTable =
        (MultipleDestinationMountTableResolver) routerContext.getRouter().getSubclusterResolver();
    Map<String, String> mapFixed = new LinkedHashMap<>();
    mapFixed.put("ns2", "/testRename");
    mapFixed.put("ns0", "/testRename");
    mapFixed.put("ns1", "/testRename");
    MountTable fixedEntry = MountTable.newInstance("/mnt", mapFixed);
    fixedEntry.setDestOrder(DestinationOrder.FIXED);
    mountTable.addEntry(fixedEntry);

    // Failure cases
    // src does not exist
    setupRename(nnFsSub0, "", true, false);
    assertThrowsWithMessage(FileNotFoundException.class,
        "Source /mnt/src/src does not exist on any namespace.",
        () -> routerClient.rename("/mnt/src/src", "/mnt/dst/dst",
            Options.Rename.OVERWRITE));
    cleanupRename();
    // dst parent does not exist
    setupRename(nnFsSub0, "src", true, false);
    assertThrowsWithMessage(FileNotFoundException.class,
        "Destination parent /mnt/dst does not exist on any namespace.",
        () -> routerClient.rename("/mnt/src/src", "/mnt/dst/dst",
            Options.Rename.OVERWRITE));
    cleanupRename();
    // dst is an existing dir
    setupRename(nnFsSub0, "src", true, false);
    setupRename(nnFsSub1, "src.file", true, true);
    setupRename(nnFsMain, "dst", false, false);
    assertThrowsWithMessage(IOException.class, "Destination already exists: /mnt/dst/dst",
        () -> routerClient.rename("/mnt/src/src", "/mnt/dst/dst",
            Options.Rename.NONE));
    assertThrowsWithMessage(IOException.class, "Destination already exists: /mnt/dst/dst",
        () -> routerClient.rename("/mnt/src/src", "/mnt/dst/dst",
            Options.Rename.OVERWRITE));
    assertThrowsWithMessage(IOException.class, "Destination already exists: /mnt/dst/dst",
        () -> routerClient.rename("/mnt/src/src.file", "/mnt/dst/dst",
            Options.Rename.OVERWRITE));
    cleanupRename();
    // dst is an existing file but OVERWRITE option is not used
    setupRename(nnFsSub0, "src.file", true, true);
    setupRename(nnFsSub0, "dst.file", false, true);
    assertThrowsWithMessage(FileAlreadyExistsException.class,
        "rename destination /testRename/dst/dst.file already exists",
        () -> routerClient.rename("/mnt/src/src.file", "/mnt/dst/dst.file",
            Options.Rename.NONE));
    cleanupRename();
    // dst is an existing file, OVERWRITE option is used but src is a directory
    setupRename(nnFsSub0, "src", true, false);
    setupRename(nnFsSub0, "dst.file", false, true);
    assertThrowsWithMessage(IOException.class,
        "Source /testRename/src/src and destination /testRename/dst/dst.file must both be directories",
        () -> routerClient.rename("/mnt/src/src", "/mnt/dst/dst.file",
            Options.Rename.OVERWRITE));
    cleanupRename();
    // dst is an existing file, OVERWRITE option is used but src is on a different ns
    setupRename(nnFsSub0, "src.file", true, true);
    setupRename(nnFsMain, "dst.file", false, true);
    assertThrowsWithMessage(IOException.class,
        "Cannot rename /mnt/src/src.file from ns0 to ns2",
        () -> routerClient.rename("/mnt/src/src.file", "/mnt/dst/dst.file",
            Options.Rename.OVERWRITE));
    cleanupRename();
    // Inconsistent src being file on one ns and dir on another
    setupRename(nnFsSub0, "src", true, true);
    setupRename(nnFsSub1, "src", true, false);
    setupRename(nnFsSub0, "", false, false);
    setupRename(nnFsSub1, "", false, false);
    assertThrowsWithMessage(IOException.class,
        "Inconsistent state with mixed files/dirs in /mnt/src/src",
        () -> routerClient.rename("/mnt/src/src", "/mnt/dst/dst0",
            Options.Rename.OVERWRITE));
    assertThrowsWithMessage(IOException.class,
        "Inconsistent state with mixed files/dirs in /mnt/src/src",
        () -> routerClient.rename("/mnt/src/src", "/mnt/dst/dst1",
            Options.Rename.OVERWRITE));
    cleanupRename();
    // Inconsistent src being multiple files on multiple namespaces
    setupRename(nnFsSub0, "src.file", true, true);
    setupRename(nnFsSub1, "src.file", true, true);
    setupRename(nnFsSub0, "", false, false);
    assertThrowsWithMessage(IOException.class,
        "Inconsistent state with the same file /mnt/src/src.file detected on multiple namespaces",
        () -> routerClient.rename("/mnt/src/src.file", "/mnt/dst/dst1",
            Options.Rename.OVERWRITE));
    cleanupRename();

    // All cases below should not fail
    // 3 cases for src being a file
    setupRename(nnFsSub0, "src.file", true, true);
    setupRename(nnFsSub1, "", false, false);
    routerClient.rename("/mnt/src/src.file", "/mnt/dst/dst.file",
        Options.Rename.NONE);
    assertTrue(nnFsSub0.getFileStatus(new Path("/testRename/dst/dst.file")).isFile());
    cleanupRename();
    // overwriting dst directly
    setupRename(nnFsSub0, "src.file", true, true);
    setupRename(nnFsSub0, "dst.file", false, true);
    routerClient.rename("/mnt/src/src.file", "/mnt/dst/dst.file",
        Options.Rename.OVERWRITE);
    assertFalse(nnFsSub0.exists(new Path("/testRename/src/src.file")));
    assertTrue(nnFsSub0.exists(new Path("/testRename/dst/dst.file")));
    cleanupRename();

    // ==============
    // Single dir src
    // dst doesn't exist, parent exists, doesn't matter which ns
    setupRename(nnFsSub0, "src", true, false);
    setupRename(nnFsMain, "", false, false);
    routerClient.rename("/mnt/src/src", "/mnt/dst/dst", Options.Rename.OVERWRITE);
    assertTrue(nnFsSub0.exists(new Path("/testRename/dst/dst/file")));
    assertFalse(nnFsMain.exists(new Path("/testRename/dst/dst")));
    cleanupRename();

    // Multiple dir src
    // dst doesn't exist, parent exists, doesn't matter which ns, doesn't matter how many parent
    setupRename(nnFsSub0, "src", true, false);
    setupRename(nnFsSub1, "src", true, false);
    setupRename(nnFsMain, "", false, false);
    routerClient.rename("/mnt/src/src", "/mnt/dst/dst", Options.Rename.OVERWRITE);
    assertTrue(nnFsSub0.exists(new Path("/testRename/dst/dst/file")));
    assertTrue(nnFsSub1.exists(new Path("/testRename/dst/dst/file")));
    assertFalse(nnFsMain.exists(new Path("/testRename/dst/dst")));
    cleanupRename();
  }

  private void assertThrowsWithMessage(Class<? extends IOException> exceptionClass, String subStr,
      ThrowingRunnable runnable) {
    try {
      runnable.run();
      fail("Should throw RemoteException or IOException.");
    } catch (RemoteException re) {
      IOException unwrappedException = re.unwrapRemoteException();
      assertEquals(exceptionClass, unwrappedException.getClass());
      assertTrue(unwrappedException.getMessage().contains(subStr));
    } catch (IOException e) {
      assertEquals(exceptionClass, e.getClass());
      assertTrue(e.getMessage().contains(subStr));
    } catch (Throwable e) {
      fail("Should throw RemoteException.");
    }
  }

  /**
   * Create necessary files/dirs for tests. Prefixes (parents) will be created if this method
   * is called at all.
   * For sources, prefix is /testRename/src/
   * For destinations, /testRename/dst/
   * E.g. path=abc/xyz as a source will create the full path /testRename/src/abc/xyz
   * @param pathStr path to create, does not contain the prefix
   */
  private static void setupRename(FileSystem fs, String pathStr, boolean isSource, boolean isFile)
      throws IOException {
    if (isSource) {
      pathStr = "/testRename/src/" + pathStr;
    } else {
      pathStr = "/testRename/dst/" + pathStr;
    }
    if (isFile) {
      fs.mkdirs(new Path(pathStr.substring(0, pathStr.lastIndexOf('/'))));
      fs.create(new Path(pathStr)).close();
      return;
    }
    fs.mkdirs(new Path(pathStr));
    // Should create an inner file if it's a source dir
    if (isSource) {
      fs.create(new Path(pathStr, "file")).close();
    }
  }

  private static void cleanupRename() throws IOException {
    reset(spyClientProtocol);
    nnFsMain.delete(new Path("/testRename"));
    nnFsSub0.delete(new Path("/testRename"));
    nnFsSub1.delete(new Path("/testRename"));
  }
}
