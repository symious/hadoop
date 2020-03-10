package org.apache.hadoop.security.sdi;

/**
 * Empty implementation of the SDICredentials interface to return null.
 */
public class EmptySDICredentials implements SDICredentials {

  @Override
  public String getUserRpcPassword() {
    return null;
  }
}
