package org.apache.hadoop.security.sdi;

import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.StringUtils;

/**
 * {@link SDICredentialsProvider} implementation that provides credentials by
 * looking at the <code>HADOOP_USER_RPCPASSWORD</code> Java system properties.
 */
public class SystemPropertiesCredentialsProvider implements SDICredentialsProvider {

  @Override
  public SDICredentials getCredentials() throws AccessControlException {
    String rpcPassword = StringUtils.trim(System.getProperty(SDI_CREDENTIAL_ENV_VAR));

    if (StringUtils.isNullOrEmpty(rpcPassword)) {

      throw new AccessControlException(
          "Unable to load SDI credentials from Java system "
              + "properties (" + SDI_CREDENTIAL_ENV_VAR + ")");
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