package org.apache.hadoop.security.sdi;

/**
 * Basic implementation of the SDICredentials interface that allows callers to
 * pass in the SDI access key and secret access in the constructor.
 */
public class BasicSDICredentials implements SDICredentials {

  private final String rpcPassword;

  /**
   * Constructs a new BasicSDICredentials object, with the specified SDI
   * user RPC password
   *
   * @param rpcPassword
   *            The SDI user RPC password.
   */
  public BasicSDICredentials(String rpcPassword) {
    if (rpcPassword == null) {
      throw new IllegalArgumentException("RPC password cannot be null.");
    }

    this.rpcPassword = rpcPassword;
  }

  @Override
  public String getUserRpcPassword() {
    return rpcPassword;
  }
}
