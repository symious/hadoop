package org.apache.hadoop.hdfs.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.ipc.RpcException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedWriter;
import java.io.IOException;

public class TestAuthProcess {
  private static MiniDFSCluster cluster = null;
  private static FileSystem fs;
  private static Configuration conf = new HdfsConfiguration();

  private final static Path TEST_ROOT = new Path("/TestHDFSAuth");

  final private static String USER1_NAME = "walter";
  final private static String GROUP1_NAME = "group1";
  final private static String RPCPASSWORD_USER1 = "I AM THE DANGER";

  private static UserGroupInformation superUser;
  private static UserGroupInformation user1;

  @BeforeClass
  public static void setUp() throws Exception {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    fs = FileSystem.get(conf);

    superUser = UserGroupInformation.getCurrentUser();
    //superUser.isBypassUser(true);
    user1 = UserGroupInformation.createUserForTesting(USER1_NAME,
        new String[] {GROUP1_NAME}, RPCPASSWORD_USER1);

    // Init test and trash root dirs in HDFS
    fs.mkdirs(TEST_ROOT);
    fs.setPermission(TEST_ROOT, new FsPermission((short) 0777));
    DFSTestUtil.verifyFilePermission(
        fs.getFileStatus(TEST_ROOT),
        superUser.getShortUserName(),
        null, FsAction.ALL, FsAction.ALL, FsAction.ALL);
  }

  @AfterClass
  public static void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }


  @Test(timeout = 60000)
  public void testSuccessAuthProcess() throws Exception {
    Configuration testConf = new Configuration(conf);
    Path user1Tmp = new Path(TEST_ROOT, "test-auth-u1");
    fs = DFSTestUtil.login(fs, testConf, user1);
    fs.mkdirs(user1Tmp);
    fs.delete(user1Tmp);
  }

  @Test(timeout = 60000)
  public void testFailAuthProcess() throws Exception {
    Configuration testConf = new Configuration(conf);
    Path user1Tmp = new Path(TEST_ROOT, "test-auth-u2");
    UserGroupInformation user1_new =
        UserGroupInformation.createUserForTesting(USER1_NAME,
            new String[] {GROUP1_NAME}, "12345678");
    try {
      fs = DFSTestUtil.login(fs, testConf, user1_new);
      fs.mkdirs(user1Tmp);
    } catch (Exception e) {
      assertTrue(e instanceof IOException);
    }
  }
}
