package org.apache.hadoop.security.sdi.profile.internal;

import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.sdi.BasicSDICredentials;
import org.apache.hadoop.security.sdi.SDICredentials;
import org.apache.hadoop.security.sdi.SDICredentialsProvider;
import org.apache.hadoop.util.StringUtils;

/**
 * Serves credentials defined in a {@link BasicProfile}. Does validation that both access key and
 * secret key exists and are non empty.
 */
public class ProfileStaticCredentialsProvider implements SDICredentialsProvider {

  private final BasicProfile profile;
  private final SDICredentialsProvider credentialsProvider;

  public ProfileStaticCredentialsProvider(BasicProfile profile) throws AccessControlException {
    this.profile = profile;
    this.credentialsProvider = new SDIStaticCredentialsProvider(fromStaticCredentials());
  }

  @Override
  public SDICredentials getCredentials() throws AccessControlException {
    return credentialsProvider.getCredentials();
  }

  @Override
  public void refresh() {
    // No Op
  }

  private SDICredentials fromStaticCredentials() throws AccessControlException {
    if (StringUtils.isNullOrEmpty(profile.getUserRpcPassword())) {
      throw new AccessControlException(String.format(
          "Unable to load credentials into profile [%s]: SDI User RPC Password is not specified.",
          profile.getUserRpcPassword()));
    }

    return new BasicSDICredentials(profile.getUserRpcPassword());
  }
}
