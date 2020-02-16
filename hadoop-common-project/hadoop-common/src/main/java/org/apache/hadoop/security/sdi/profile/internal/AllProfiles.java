package org.apache.hadoop.security.sdi.profile.internal;

import java.util.Collections;
import java.util.Map;

/**
 * Simple wrapper around a map of profiles.
 */
public class AllProfiles {

  private final Map<String, BasicProfile> profiles;

  public AllProfiles(Map<String, BasicProfile> profiles) {
    this.profiles = profiles;
  }

  public Map<String, BasicProfile> getProfiles() {
    return Collections.unmodifiableMap(profiles);
  }

  public BasicProfile getProfile(String profileName) {
    return profiles.get(profileName);
  }
}
