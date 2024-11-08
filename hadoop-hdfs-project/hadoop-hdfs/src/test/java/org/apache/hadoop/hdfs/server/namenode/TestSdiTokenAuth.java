package org.apache.hadoop.hdfs.server.namenode;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.HashSet;

import com.shopee.di.datasuite.auth.client.exception.OAuthException;
import com.shopee.di.datasuite.auth.client.token.TokenInfo;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.NullGroupsMapping;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.anyString;

public class TestSdiTokenAuth {

  @Test
  public void testSdiTokenAuth() throws IOException, InterruptedException {
    // Set up mock client that parses csv token
    Server.TokenClient mockClient = Mockito.mock(Server.TokenClient.class);
    Mockito.doAnswer(invocationOnMock -> {
      try {
        String token = invocationOnMock.getArgument(0);
        String[] split = token.split(",");
        String user = split[0];
        boolean failed = split[1].equals("failed");
        if (failed) {
          throw new OAuthException(new Exception("test failure"));
        }
        TokenInfo st = new TokenInfo("unittest", user, new HashSet<>());
        return st;
      } catch (Exception e) {
        throw new IOException("Invalid token", e);
      }
    }).when(mockClient).decrypt(anyString());

    // Write some dummy test users
    File tempFile = File.createTempFile(GenericTestUtils.getMethodName(), null);
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(tempFile))) {
      bw.write("hdfs,password,true\n");
      bw.write("alice,password1,false\n");
      bw.write("bob,password2,false");
    }
    // Start with hdfs user to set up cluster first
    System.setProperty("HADOOP_USER_NAME", "hdfs");
    System.setProperty("HADOOP_USER_RPCPASSWORD", "password");

    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE,
        tempFile.getAbsolutePath());
    conf.setLong(
        CommonConfigurationKeys.HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE_CACHE_REFRESH_INTERVAL, 1);
    conf.setBoolean(CommonConfigurationKeys.HADOOP_CLIENT_RPC_SDI_AUTHENTICATION_ENABLED_KEY, true);
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE,
        tempFile.getAbsolutePath());
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_UNIFIED_AUTH_CLIENT_KEY,
        new Base64(0).encodeToString(new byte[32]));
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_GROUP_MAPPING,
        NullGroupsMapping.class.getName());
    UserGroupInformation.setConfiguration(conf);

    MiniDFSCluster clusterRef = null;

    try {
      final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
      clusterRef = cluster;
      cluster.waitActive();
      ((NameNodeRpcServer) cluster.getNameNode().getRpcServer()).getClientRpcServer()
          .setSdiTokenClientForTesting(mockClient);

      // Generate test data
      DistributedFileSystem baseFs = cluster.getFileSystem();
      Path pathA = new Path("/pathA");
      Path pathB = new Path("/pathB");
      baseFs.create(pathA).close();
      baseFs.create(pathB).close();
      baseFs.setOwner(pathA, "alice", "alice");
      baseFs.setOwner(pathB, "bob", "bob");
      baseFs.setPermission(pathA, FsPermission.createImmutable((short) 0700));
      baseFs.setPermission(pathB, FsPermission.createImmutable((short) 0700));

      // Invalid token, not csv, should fail with exception
      UserGroupInformation ugi = UserGroupInformation.createRemoteUser("bob", "invalidtoken", null);
      try {
        ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
          DistributedFileSystem fs = cluster.getFileSystem();
          fs.open(pathA).close();
          return null;
        });
        Assert.fail("Should fail");
      } catch (IOException ioe) {
        Assert.assertTrue(ioe.getMessage().contains("Invalid token"));
      }

      // Valid token format, token fails auth
      ugi = UserGroupInformation.createRemoteUser("bob", "bob,failed", null);
      try {
        ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
          DistributedFileSystem fs = cluster.getFileSystem();
          fs.open(pathA).close();
          return null;
        });
      } catch (IOException ioe) {
        Assert.assertTrue(ioe.getMessage().contains("Invalid token"));
      }

      // Valid token format, token passes auth, should log in as bob
      ugi = UserGroupInformation.createRemoteUser("bob", "bob,1234", null);
      // Should fail due to permission
      try {
        ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
          DistributedFileSystem fs = cluster.getFileSystem();
          fs.open(pathA).close();
          return null;
        });
      } catch (AccessControlException ace) {
        Assert.assertTrue(ace.getMessage().contains(
            "Permission denied: user=bob, access=READ, inode=\"/pathA\":alice:alice:-rwx------"));
      }
      // Shouldn't fail when reading /pathB
      ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
        DistributedFileSystem fs = cluster.getFileSystem();
        fs.open(pathB).close();
        return null;
      });

    } finally {
      if (clusterRef != null) {
        clusterRef.shutdown();
      }
    }
  }
}
