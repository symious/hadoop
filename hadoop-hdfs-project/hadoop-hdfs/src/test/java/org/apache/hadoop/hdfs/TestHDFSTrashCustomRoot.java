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

package org.apache.hadoop.hdfs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestHDFSTrashCustomRoot {

  public static final Log LOG = LogFactory.getLog(TestHDFSTrashCustomRoot.class);

  private static MiniDFSCluster cluster = null;
  private static FileSystem fs;
  private static final Configuration conf = new HdfsConfiguration();

  private final static Path TEST_ROOT = new Path("/TestHDFSTrash-ROOT");
  private final static String BASE_TRASH_ROOT_STR = "/Trash";
  private final static Path BASE_TRASH_ROOT = new Path(BASE_TRASH_ROOT_STR);

  final private static String GROUP1_NAME = "group1";
  final private static String GROUP2_NAME = "group2";
  final private static String GROUP3_NAME = "group3";
  final private static String USER1_NAME = "user1";
  final private static String USER2_NAME = "user2";

  private static UserGroupInformation user1;
  private static UserGroupInformation user2;

  @BeforeClass
  public static void setUp() throws Exception {
    conf.set(CommonConfigurationKeys.FS_TRASH_ROOT, BASE_TRASH_ROOT_STR + "/${user.name}");
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    fs = FileSystem.get(conf);

    UserGroupInformation superUser = UserGroupInformation.getCurrentUser();
    user1 = UserGroupInformation.createUserForTesting(USER1_NAME,
        new String[] {GROUP1_NAME, GROUP2_NAME});
    user2 = UserGroupInformation.createUserForTesting(USER2_NAME,
        new String[] {GROUP2_NAME, GROUP3_NAME});

    // Init test and trash root dirs in HDFS
    fs.mkdirs(TEST_ROOT);
    fs.setPermission(TEST_ROOT, new FsPermission((short) 0777));
    DFSTestUtil.verifyFilePermission(
        fs.getFileStatus(TEST_ROOT),
        superUser.getShortUserName(),
        null, FsAction.ALL, FsAction.ALL, FsAction.ALL);

    fs.mkdirs(BASE_TRASH_ROOT);
    fs.setPermission(BASE_TRASH_ROOT, new FsPermission((short) 0777));
    DFSTestUtil.verifyFilePermission(
        fs.getFileStatus(BASE_TRASH_ROOT),
        superUser.getShortUserName(),
        null, FsAction.ALL, FsAction.ALL, FsAction.ALL);

    fs.mkdirs(new Path("/user"));
    fs.setPermission(new Path("/user"), new FsPermission((short) 0777));
    DFSTestUtil.verifyFilePermission(
        fs.getFileStatus(BASE_TRASH_ROOT),
        superUser.getShortUserName(),
        null, FsAction.ALL, FsAction.ALL, FsAction.ALL);
  }

  @AfterClass
  public static void tearDown() {
    if (cluster != null) { cluster.shutdown(); }
  }

  @Test
  public void testDeleteTrash() throws Exception {
    Configuration testConf = new Configuration(conf);
    testConf.set(CommonConfigurationKeys.FS_TRASH_ROOT, BASE_TRASH_ROOT_STR + "/${user.name}");
    testConf.set(CommonConfigurationKeys.FS_TRASH_INTERVAL_KEY, "10");

    Path user1Tmp = new Path(TEST_ROOT, "test-del-u1");
    Path user2Tmp = new Path(TEST_ROOT, "test-del-u2");

    // login as user1, move something to trash
    // verify user1 can remove its own trash dir
    fs = DFSTestUtil.login(fs, testConf, user1);
    fs.mkdirs(user1Tmp);
    Trash u1Trash = getPerUserTrash(user1, fs, testConf);
    Path u1t = u1Trash.getCurrentTrashDir(user1Tmp);
    assertTrue(String.format("Failed to move %s to trash", user1Tmp),
        u1Trash.moveToTrash(user1Tmp));
    assertTrue(
        String.format(
            "%s should be allowed to remove its own trash directory %s",
            user1.getUserName(), u1t),
        fs.delete(u1t, true));
    assertFalse(fs.exists(u1t));

    // login as user2, move something to trash
    fs = DFSTestUtil.login(fs, testConf, user2);
    fs.mkdirs(user2Tmp);
    Trash u2Trash = getPerUserTrash(user2, fs, testConf);
    u2Trash.moveToTrash(user2Tmp);
    Path u2t = u2Trash.getCurrentTrashDir(user2Tmp);

    try {
      // user1 should not be able to remove user2's trash dir
      fs = DFSTestUtil.login(fs, testConf, user1);
      fs.delete(u2t, true);
      fail(String.format("%s should not be able to remove %s trash directory",
          USER1_NAME, USER2_NAME));
    } catch (AccessControlException e) {
      assertTrue("Permission denied messages must carry the username",
          e.getMessage().contains(USER1_NAME));
    }
  }

  @Test
  public void testMismatchedClientConfig() throws Exception {
    Configuration testConf = new Configuration(conf);
    testConf.set(CommonConfigurationKeys.FS_TRASH_INTERVAL_KEY, "1");
    testConf.set(CommonConfigurationKeys.FS_TRASH_CHECKPOINT_INTERVAL_KEY, "0.1"); // 6 seconds

    Path user1Tmp = new Path(TEST_ROOT, "test-del-u1");
    fs = DFSTestUtil.login(fs, testConf, user1);

    // Client using new config
    fs.mkdirs(user1Tmp);
    Trash u1Trash = getPerUserTrash(user1, fs, testConf, 0, false);
    Path u1t = u1Trash.getCurrentTrashDir(user1Tmp);
    assertTrue(String.format("Failed to move %s to trash", user1Tmp),
        u1Trash.moveToTrash(user1Tmp));
    assertTrue(fs.exists(u1t));

    // Client using old config
    fs.mkdirs(user1Tmp);
    Trash u1Trash2 = getPerUserTrash(user1, fs, testConf, 1, false);
    Path u1t2 = u1Trash2.getCurrentTrashDir(user1Tmp);
    assertTrue(String.format("Failed to move %s to trash", user1Tmp),
        u1Trash2.moveToTrash(user1Tmp));
    assertTrue(fs.exists(u1t2));

    Runnable emptier = u1Trash2.getEmptier();
    Thread emptierThread = new Thread(emptier);
    emptierThread.start();

    // Now wait for 8s
    Thread.sleep(8000);
    // No rogue Current
    for (FileStatus trashRoot : fs.getTrashRoots(true)) {
      assertFalse(trashRoot.toString().endsWith("Current"));
    }
    emptierThread.interrupt();
    emptierThread.join();
  }

  @Test
  public void testTrashEmptier() throws Exception {
    Configuration config = new Configuration(conf);
    // Trash with 12 second deletes and 6 seconds checkpoints
    config.set(CommonConfigurationKeys.FS_TRASH_INTERVAL_KEY, "0.2"); // 12 seconds
    config.set(CommonConfigurationKeys.FS_TRASH_CHECKPOINT_INTERVAL_KEY, "0.1"); // 6 seconds

    fs = DFSTestUtil.login(fs, config, user2);
    Trash trash = new Trash(config);

    // Start Emptier in background
    Runnable emptier = trash.getEmptier();
    Thread emptierThread = new Thread(emptier);
    emptierThread.start();

    // First create a new directory with mkdirs
    Path myPath = new Path(TEST_ROOT, "testEmptier");
    fs.mkdirs(myPath);
    int fileIndex = 0;
    Set<String> checkpoints = new HashSet<>();
    // Create trash out here since each getPerUserTrash makes a new user name
    Trash ut = getPerUserTrash(user2, fs, config);
    while (true)  {
      // Create a file with a new name
      Path myFile = new Path(TEST_ROOT, "testEmptier/" + fileIndex++);
      fs.create(myFile).close();

      // Delete the file to trash
      ut.moveToTrash(myFile);

      FileStatus[] files = fs.listStatus(ut.getCurrentTrashDir(myFile).getParent());
      // Scan files in .Trash and add them to set of checkpoints
      for (FileStatus file : files) {
        String fileName = file.getPath().getName();
        checkpoints.add(fileName);
      }
      // If checkpoints has 4 objects it is Current + 3 checkpoint directories
      if (checkpoints.size() == 4) {
        // The actual contents should be smaller since the last checkpoint
        // should've been deleted and Current might not have been recreated yet
        assertTrue(checkpoints.size() > files.length);
        break;
      }
      Thread.sleep(5000);
    }
    emptierThread.interrupt();
    emptierThread.join();
  }


  /**
   * Return a {@link Trash} instance using giving configuration.
   * The trash root directory is set to an unique directory under
   * {@link #BASE_TRASH_ROOT}. Use this method to isolate trash
   * directories for different users.
   */
  private Trash getPerUserTrash(UserGroupInformation ugi,
      FileSystem fileSystem, Configuration config, int opt,
      boolean randomId) throws IOException {
    // generate an unique path per instance
    UUID trashId;
    if (randomId) {
      trashId = UUID.randomUUID();
    } else {
      trashId = UUID.nameUUIDFromBytes(ugi.getShortUserName().getBytes());
    }
    StringBuffer sb = new StringBuffer()
        .append(ugi.getUserName())
        .append("-")
        .append(trashId.toString());
    Path userTrashRoot;
    if (opt == 0) {
      userTrashRoot = new Path(BASE_TRASH_ROOT, sb.toString());
    } else {
      userTrashRoot = new Path("/user/" + sb.toString(), ".Trash");
    }
    FileSystem spyUserFs = Mockito.spy(fileSystem);
    Mockito.when(spyUserFs.getTrashRoot(Mockito.any(Path.class)))
        .thenReturn(userTrashRoot);
    return new Trash(spyUserFs, config);
  }

  private Trash getPerUserTrash(UserGroupInformation ugi,
      FileSystem fileSystem, Configuration config) throws IOException {
    return getPerUserTrash(ugi, fileSystem, config, 0, true);
  }
}