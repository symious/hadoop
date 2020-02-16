package org.apache.hadoop.security.sdi;

import org.apache.hadoop.security.sdi.profile.ProfileCredentialsProvider;

public class DefaultSDICredentialsProviderChain extends SDICredentialsProviderChain {

  private static final DefaultSDICredentialsProviderChain INSTANCE
      = new DefaultSDICredentialsProviderChain();

  public DefaultSDICredentialsProviderChain() {
    super(new EnvironmentVariableCredentialsProvider(),
        new SystemPropertiesCredentialsProvider(),
        new ProfileCredentialsProvider());
  }

  public static DefaultSDICredentialsProviderChain getInstance() {
    return INSTANCE;
  }
}
