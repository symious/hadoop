package org.apache.hadoop.security.sdi;

/**
 * Provides access to the SDI credentials used for accessing SDI services: SDI
 * user RPC password. These credentials are used to securely
 * sign requests to SDI services.
 * <p>
 * A basic implementation of this interface is provided in {@link BasicSDICredentials}
 */
public interface SDICredentials {

  /**
   * Returns the SDI user RPC password for this credentials object.
   *
   * @return The SDI user RPC password for this credentials object.
   */
  String getUserRpcPassword();

}
