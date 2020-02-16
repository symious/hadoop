package org.apache.hadoop.security.sdi.profile.internal;

import java.util.Collections;
import java.util.Map;

/**
 * Represents a CLI style config profile with a name and simple properties. Provides convenient
 * access to the properties the Java SDK deals with and also raw access to all properties.
 */
public class BasicProfile {

  private final static String HADOOP_USER_RPCPASSWORD = "HADOOP_USER_RPCPASSWORD";

  private final String profileName;
  private final Map<String, String> properties;

  public BasicProfile(String profileName,
      Map<String, String> properties) {
    this.profileName = profileName;
    this.properties = properties;
  }

  /**
   * @return The name of this profile.
   */
  public String getProfileName() {
    return profileName;
  }

  /**
   * Returns a map of profile properties included in this Profile instance. The returned
   * properties corresponds to how this profile is described in the credential profiles file,
   * i.e., profiles with basic credentials consist of two properties {"aws_access_key_id",
   * "aws_secret_access_key"} and profiles with session credentials have three properties, with an
   * additional "aws_session_token" property.
   */
  public Map<String, String> getProperties() {
    return Collections.unmodifiableMap(properties);
  }

  /**
   * Returns the value of a specific property that is included in this Profile instance.
   *
   * @see BasicProfile#getProperties()
   */
  public String getPropertyValue(String propertyName) {
    return getProperties().get(propertyName);
  }

  public String getUserRpcPassword() {
    return getPropertyValue(HADOOP_USER_RPCPASSWORD);
  }
}
