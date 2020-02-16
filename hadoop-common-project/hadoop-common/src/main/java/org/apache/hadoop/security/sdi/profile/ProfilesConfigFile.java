package org.apache.hadoop.security.sdi.profile;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.sdi.SDICredentials;
import org.apache.hadoop.security.sdi.SDICredentialsProvider;
import org.apache.hadoop.security.sdi.profile.internal.AllProfiles;
import org.apache.hadoop.security.sdi.profile.internal.BasicProfile;
import org.apache.hadoop.security.sdi.profile.internal.BasicProfileConfigLoader;
import org.apache.hadoop.security.sdi.profile.internal.ProfileStaticCredentialsProvider;
import org.apache.hadoop.util.Check;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads the local SDI credential profiles from the standard location (~/.sdi/credentials), which
 * can be easily overridden through the <code>SDI_CREDENTIAL_PROFILES_FILE</code> environment
 * variable or by specifying an alternate credentials file location through this class' constructor.
 * <p> Currently the SDI credentials file only allows one profile named "default".
 * <pre>
 * [default]
 * HADOOP_USER_RPCPASSWORD=userRpcPassword
 * </pre>
 *
 */
public class ProfilesConfigFile {

  static final Logger LOG = LoggerFactory.getLogger(ProfilesConfigFile.class);

  private final File profileFile;
  /**
   * Cache credential providers as credentials from profiles are requested. Doesn't really make a
   * difference for basic credentials but for assume role it's more efficient as each assume role
   * provider has it's own async refresh logic.
   */
  private final ConcurrentHashMap<String, SDICredentialsProvider> credentialProviderCache =
      new ConcurrentHashMap<>();
  private volatile AllProfiles allProfiles;
  private volatile long profileFileLastModified;

  /**
   * Loads the SDI credential profiles file from the default location (~/.sdi/credentials) or from
   * an alternate location if <code>SDI_CREDENTIAL_PROFILES_FILE</code> is set.
   */
  public ProfilesConfigFile() throws AccessControlException {
    this(getCredentialProfilesFile());
  }

  /**
   * Loads the SDI credential profiles from the file. The path of the file is specified as a
   * parameter to the constructor.
   */
  public ProfilesConfigFile(String filePath) throws AccessControlException {
    this(new File(validateFilePath(filePath)));
  }

  private static String validateFilePath(String filePath) {
    if (filePath == null) {
      throw new IllegalArgumentException(
          "Unable to load SDI profiles: specified file path is null.");
    }
    return filePath;
  }

  /**
   * Loads the SDI credential profiles from the file. The reference to the file is specified as a
   * parameter to the constructor.
   */
  public ProfilesConfigFile(File file) throws AccessControlException {
    profileFile = Check.notNull(file, "profile file");
    profileFileLastModified = file.lastModified();
    allProfiles = loadProfiles(profileFile);
  }

  /**
   * Returns the SDI credentials for the specified profile.
   */
  public SDICredentials getCredentials(String profileName) throws AccessControlException {
    final SDICredentialsProvider provider = credentialProviderCache.get(profileName);
    if (provider != null) {
      return provider.getCredentials();
    } else {
      BasicProfile profile = allProfiles.getProfile(profileName);
      if (profile == null) {
        throw new IllegalArgumentException("No SDI profile named '" + profileName + "'");
      }
      final SDICredentialsProvider newProvider = fromProfile(profile);
      credentialProviderCache.put(profileName, newProvider);
      return newProvider.getCredentials();
    }
  }

  /**
   * Reread data from disk.
   */
  public void refresh() {
    if (profileFile.lastModified() > profileFileLastModified) {
      synchronized (this) {
        if (profileFile.lastModified() > profileFileLastModified) {
          try {
            allProfiles = loadProfiles(profileFile);
            profileFileLastModified = profileFile.lastModified();
          } catch (Exception e) {
            LOG.warn("Unable to refresh SDI credentials: " + e.getMessage());
          }
        }
      }
    }

    credentialProviderCache.clear();
  }

  public Map<String, BasicProfile> getAllBasicProfiles() {
    return allProfiles.getProfiles();
  }

  private static File getCredentialProfilesFile() throws AccessControlException {
    return (new CredentialsDefaultLocationProvider()).getLocation();
  }

  private static AllProfiles loadProfiles(File file) throws AccessControlException {
    return BasicProfileConfigLoader.INSTANCE.loadProfiles(file);
  }

  private SDICredentialsProvider fromProfile(BasicProfile profile) throws AccessControlException {
    return new ProfileStaticCredentialsProvider(profile);
  }

  /**
   * Load shared credentials file from the default location (~/.aws/credentials).
   */
  static private class CredentialsDefaultLocationProvider {

    private static final String DEFAULT_CREDENTIAL_PROFILES_FILENAME = "credentials";

    public File getLocation() throws AccessControlException {
      File credentialProfiles = new File(getSdiDirectory(), DEFAULT_CREDENTIAL_PROFILES_FILENAME);
      if (credentialProfiles.exists() && credentialProfiles.isFile()) {
        return credentialProfiles;
      }
      return null;
    }
  }

  /**
   * @return File of ~/.sdi directory.
   */
  static private File getSdiDirectory() throws AccessControlException {
    return new File(getHomeDirectory(), ".sdi");
  }

  static private String getHomeDirectory() throws AccessControlException {
    String userHome = System.getProperty("user.home");
    if (userHome == null) {
      throw new AccessControlException(
          "Unable to load SDI profiles: " + "'user.home' System property is not set.");
    }
    return userHome;
  }
}
