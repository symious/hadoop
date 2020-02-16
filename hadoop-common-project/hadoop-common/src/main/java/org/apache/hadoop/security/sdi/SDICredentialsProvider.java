package org.apache.hadoop.security.sdi;

import org.apache.hadoop.security.AccessControlException;

/**
 * Interface for providing SDI credentials.
 *
 * A mimic of AWSCredentialsProvider
 *
 */
public interface SDICredentialsProvider {

  String SDI_CREDENTIAL_ENV_VAR = "HADOOP_USER_RPCPASSWORD";

  /**
   * Returns SDICredentials which the caller can use to authorize an SDI request.
   * Each implementation of SDICredentialsProvider can chose its own strategy for
   * loading credentials.  For example, an implementation might load credentials
   * from an existing key management system, or load new credentials when
   * credentials are rotated.
   *
   * @return SDICredentials which the caller can use to authorize an SDI request.
   */
  SDICredentials getCredentials() throws AccessControlException;

  /**
   * Forces this credentials provider to refresh its credentials. For many
   * implementations of credentials provider, this method may simply be a
   * no-op, such as any credentials provider implementation that vends
   * static/non-changing credentials. For other implementations that vend
   * different credentials through out their lifetime, this method should
   * force the credentials provider to refresh its credentials.
   */
  void refresh();

}
