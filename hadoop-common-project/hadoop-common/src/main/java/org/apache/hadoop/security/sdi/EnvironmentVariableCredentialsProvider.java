package org.apache.hadoop.security.sdi;


import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.StringUtils;

public class EnvironmentVariableCredentialsProvider implements SDICredentialsProvider {

  @Override
  public SDICredentials getCredentials() throws AccessControlException {

    String rpcPassword = StringUtils.trim(
        System.getenv(SDI_CREDENTIAL_ENV_VAR));
    if (StringUtils.isNullOrEmpty(rpcPassword)) {
      rpcPassword = StringUtils.trim(
          System.getProperty(SDI_CREDENTIAL_ENV_VAR));
      if (StringUtils.isNullOrEmpty(rpcPassword)) {
        throw new AccessControlException(
            "Unable to load SDI credentials from environment variables " +
                "(" + SDI_CREDENTIAL_ENV_VAR + ")");
      }
    }

    return new BasicSDICredentials(rpcPassword);
  }

  @Override
  public void refresh() {
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}