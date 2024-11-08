package org.apache.hadoop.security.sdi;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.AccessControlException;

public class SdiCredentialsUtil {

  public static final String HADOOP_USER_TOKEN = "HADOOP_USER_TOKEN";
  public static final Text HADOOP_USER_TOKEN_TEXT = new Text("HADOOP_USER_TOKEN");
  private static final SDICredentialsProvider sdiCredentialsProvider =
          DefaultSDICredentialsProviderChain.getInstance();

  // Get User RpcPassword, return null if not found
  public static String getSdiUserRpcPassword() {
    String sdiUserRpcpassword = null;
    try {
      sdiUserRpcpassword = sdiCredentialsProvider.getCredentials().getUserRpcPassword();
    } catch (AccessControlException e) {
      e.printStackTrace();
    }
    return sdiUserRpcpassword;
  }

  public static String getSdiUserToken() {
    String sdiToken = System.getenv(HADOOP_USER_TOKEN);
    if (sdiToken == null) {
      sdiToken = System.getProperty(HADOOP_USER_TOKEN);
    }
    return sdiToken;
  }

}
