package org.apache.hadoop.security.sdi;

import org.apache.hadoop.security.AccessControlException;

public class SdiCredentialsUtil {

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

}
