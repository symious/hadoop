package org.apache.hadoop.security.sdi.profile.internal;

import org.apache.hadoop.security.sdi.SDICredentials;
import org.apache.hadoop.security.sdi.SDICredentialsProvider;
import org.apache.hadoop.util.Check;

/**
 * Simple implementation of SDICredentialsProvider that just wraps static SDICredentials.
 */
public class SDIStaticCredentialsProvider implements SDICredentialsProvider {

  private final SDICredentials credentials;

  public SDIStaticCredentialsProvider(SDICredentials credentials) {
    this.credentials = Check.notNull(credentials, "credentials");
  }

  public SDICredentials getCredentials() {
    return credentials;
  }

  public void refresh() {
  }

}
