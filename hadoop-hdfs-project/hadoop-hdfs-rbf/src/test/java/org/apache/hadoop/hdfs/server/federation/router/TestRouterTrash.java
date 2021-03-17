package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.resolver.MultipleDestinationMountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.order.DestinationOrder;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_ROOT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestRouterTrash {

  /**
   * Number of namespaces.
   */
  private static final int NUM_NAMESPACES = 2;


  /**
   * Mini HDFS clusters with Routers and State Store.
   */
  private static StateStoreDFSCluster cluster;
  /**
   * Router for testing.
   */
  private static MiniRouterDFSCluster.RouterContext routerContext;
  /**
   * Router/federated filesystem.
   */
  private static FileSystem routerFs;
  /**
   * Filesystem for each namespace.
   */
  private static List<FileSystem> nsFss = new LinkedList<>();


  @Before
  public void setup() throws Exception {
    // 2 nameservices with 1 namenode each (no HA needed for this test)
    cluster = new StateStoreDFSCluster(
        false, NUM_NAMESPACES, MultipleDestinationMountTableResolver.class);

    // Start NNs and DNs and wait until ready
    cluster.startCluster();

    // Build and start a Router with: State Store + Admin + RPC
    Configuration routerConf = new RouterConfigBuilder()
        .stateStore()
        .admin()
        .rpc()
        .build();
    routerConf.set(CommonConfigurationKeys.FS_TRASH_INTERVAL_KEY, "9999");
    routerConf.setStrings(FS_TRASH_ROOT, "/randompath");
    cluster.addRouterOverrides(routerConf);
    cluster.startRouters();
    routerContext = cluster.getRandomRouter();

    // Register and verify all NNs with all routers
    cluster.registerNamenodes();
    cluster.waitNamenodeRegistration();

    // Get filesystems for federated and each namespace
    routerFs = routerContext.getFileSystem();
    for (String nsId : cluster.getNameservices()) {
      List<MiniRouterDFSCluster.NamenodeContext> nns =
          cluster.getNamenodes(nsId);
      for (MiniRouterDFSCluster.NamenodeContext nn : nns) {
        FileSystem nnFs = nn.getFileSystem();
        nsFss.add(nnFs);
      }
    }
    assertEquals(NUM_NAMESPACES, nsFss.size());
  }

  @After
  public void cleanup() {
    cluster.shutdown();
    cluster = null;
    routerContext = null;
    routerFs = null;
    nsFss.clear();
  }


  @Test
  public void testCrossNamespaceTrashOperation() throws Exception {

    nsFss.get(0).mkdirs(new Path("/ns0toremoved"));
    nsFss.get(1).mkdirs(new Path("/ns1toremoved"));
    createMountTableEntry("/ns0toremoved", DestinationOrder.RANDOM, "ns0");
    createMountTableEntry("/ns1toremoved", DestinationOrder.RANDOM, "ns1");
    createTestFile(routerFs, "/ns0toremoved/file0.txt");
    createTestFile(routerFs, "/ns1toremoved/file1.txt");

    // Run trash from router
    Trash trash = new Trash(routerFs, routerFs.getConf());
    Path trashRoot = trash.getCurrentTrashDir(new Path("/"));
    String user = trashRoot.toString().split(":")[2].split("/")[2];
    // Should pass
    assertPathStatus("/ns0toremoved/file0.txt", 0, true);
    trash.moveToTrash(new Path("/ns0toremoved/file0.txt"));
    assertPathStatus("/ns0toremoved/file0.txt", 0, false);
    assertPathStatus("/ns0toremoved/file0.txt", 1, false);
    assertPathStatus("/Trash/" + user + "/Current/ns0toremoved/file0.txt", 0, true);
    assertPathStatus("/Trash/" + user + "/Current/ns0toremoved/file0.txt", 1, false);
    // Should also pass
    assertPathStatus("/ns1toremoved/file1.txt", 1, true);
    trash.moveToTrash(new Path("/ns1toremoved/file1.txt"));
    assertPathStatus("/ns1toremoved/file1.txt", 0, false);
    assertPathStatus("/ns1toremoved/file1.txt", 1, false);
    assertPathStatus("/Trash/" + user + "/Current/ns1toremoved/file1.txt", 0, false);
    assertPathStatus("/Trash/" + user + "/Current/ns1toremoved/file1.txt", 1, true);
  }

  private void assertPathStatus(String path, int ns, boolean found)
      throws IOException {
    if (found) {
      Assert.assertNotNull(nsFss.get(ns).getFileStatus(
          new Path(path)));
    } else {
      try {
        nsFss.get(ns).getFileStatus(
            new Path(path));
      } catch (FileNotFoundException ignored) { }
    }
  }

  private static void createTestFile(
      final FileSystem fs, final String filename) throws IOException {

    final Path path = new Path(filename);

    // Write the data
    FSDataOutputStream os = fs.create(path);
    os.writeUTF("Test data " + filename);
    os.close();

    // Read the data and check
    FSDataInputStream is = fs.open(path);
    String read = is.readUTF();
    assertEquals("Test data " + filename, read);
    is.close();
  }

  private void createMountTableEntry(
      final String mountPoint, final DestinationOrder order, String ns)
      throws Exception {

    RouterClient admin = routerContext.getAdminClient();
    MountTableManager mountTable = admin.getMountTableManager();
    Map<String, String> destMap = new HashMap<>();
    if (ns == null) {
      for (String nsId : cluster.getNameservices()) {
        destMap.put(nsId, mountPoint);
      }
    } else {
      destMap.put(ns, mountPoint);
    }
    MountTable newEntry = MountTable.newInstance(mountPoint, destMap);
    newEntry.setDestOrder(order);
    AddMountTableEntryRequest addRequest =
        AddMountTableEntryRequest.newInstance(newEntry);
    AddMountTableEntryResponse addResponse =
        mountTable.addMountTableEntry(addRequest);
    boolean created = addResponse.getStatus();
    assertTrue(created);

    // Refresh the caches to get the mount table
    Router router = routerContext.getRouter();
    StateStoreService stateStore = router.getStateStore();
    stateStore.refreshCaches(true);

    // Check for the path
    GetMountTableEntriesRequest getRequest =
        GetMountTableEntriesRequest.newInstance(mountPoint);
    GetMountTableEntriesResponse getResponse =
        mountTable.getMountTableEntries(getRequest);
    List<MountTable> entries = getResponse.getEntries();
    assertEquals(1, entries.size());
    assertEquals(mountPoint, entries.get(0).getSourcePath());
  }
}
