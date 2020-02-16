package org.apache.hadoop.security.sdi.profile.internal;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to load a CLI style config or credentials file. Performs only basic validation on
 * properties and profiles.
 */
public class BasicProfileConfigLoader {

  private static final Logger LOG = LoggerFactory.getLogger(BasicProfileConfigLoader.class);

  public static final BasicProfileConfigLoader INSTANCE = new BasicProfileConfigLoader();

  private BasicProfileConfigLoader() {
  }

  public AllProfiles loadProfiles(File file) throws AccessControlException {
    if (file == null) {
      throw new IllegalArgumentException(
          "Unable to load SDI profiles: specified file is null.");
    }

    if (!file.exists() || !file.isFile()) {
      throw new IllegalArgumentException(
          "SDI credential profiles file not found in the given path: " +
              file.getAbsolutePath());
    }

    FileInputStream fis = null;
    try {
      fis = new FileInputStream(file);
      return loadProfiles(fis);
    } catch (IOException ioe) {
      throw new AccessControlException(
          "Unable to load SDI credential profiles file at: " + file.getAbsolutePath());
    } finally {
      if (fis != null) {
        try {
          fis.close();
        } catch (IOException ioe) {
        }
      }
    }
  }

  /**
   * Loads the credential profiles from the given input stream.
   *
   * @param is input stream from where the profile details are read.
   */
  private AllProfiles loadProfiles(InputStream is) throws IOException {
    ProfilesConfigFileLoaderHelper helper = new ProfilesConfigFileLoaderHelper();
    Map<String, Map<String, String>> allProfileProperties = helper
        .parseProfileProperties(new Scanner(is, StringUtils.UTF8.name()));

    // Convert the loaded property map to credential objects
    Map<String, BasicProfile> profilesByName = new LinkedHashMap<String, BasicProfile>();

    for (Entry<String, Map<String, String>> entry : allProfileProperties.entrySet()) {
      String profileName = entry.getKey();
      Map<String, String> properties = entry.getValue();

      if (profileName.startsWith("profile ")) {
        LOG.warn(
            "Your profile name includes a 'profile ' prefix. This is considered part of the profile name in the " +
                "Java SDK, so you will need to include this prefix in your profile name when you reference this " +
                "profile from your Java code.");
      }

      assertParameterNotEmpty(profileName,
          "Unable to load properties from profile: Profile name is empty.");
      profilesByName.put(profileName, new BasicProfile(profileName, properties));
    }

    return new AllProfiles(profilesByName);
  }

  /**
   * <p> Asserts that the specified parameter value is neither <code>empty</code> nor null, and if
   * it is, throws a <code>SdkClientException</code> with the specified error message. </p>
   *
   * @param parameterValue The parameter value being checked.
   * @param errorMessage   The error message to include in the SdkClientException if the
   *                       specified parameter value is empty.
   */
  private void assertParameterNotEmpty(String parameterValue, String errorMessage)
      throws AccessControlException {
    if (StringUtils.isNullOrEmpty(parameterValue)) {
      throw new AccessControlException(errorMessage);
    }
  }

  /**
   * Implementation of AbstractProfilesConfigFileScanner that groups profile properties into a map
   * while scanning through the credentials profile.
   */
  private static class ProfilesConfigFileLoaderHelper extends AbstractProfilesConfigFileScanner {

    /**
     * Map from the parsed profile name to the map of all the property values included the
     * specific profile
     */
    protected final Map<String, Map<String, String>> allProfileProperties = new LinkedHashMap<String, Map<String, String>>();

    /**
     * Parses the input and returns a map of all the profile properties.
     */
    public Map<String, Map<String, String>> parseProfileProperties(Scanner scanner) {
      allProfileProperties.clear();
      run(scanner);
      return new LinkedHashMap<String, Map<String, String>>(allProfileProperties);
    }

    @Override
    protected void onEmptyOrCommentLine(String profileName, String line) {
      // Ignore empty or comment line
    }

    @Override
    protected void onProfileStartingLine(String newProfileName, String line) {
      // If the same profile name has already been declared, clobber the
      // previous one
      allProfileProperties.put(newProfileName, new HashMap<String, String>());
    }

    @Override
    protected void onProfileEndingLine(String prevProfileName) {
      // No-op
    }

    @Override
    protected void onProfileProperty(String profileName, String propertyKey,
        String propertyValue, boolean isSupportedProperty,
        String line) {
      Map<String, String> properties = allProfileProperties.get(profileName);

      if (properties.containsKey(propertyKey)) {
        throw new IllegalArgumentException(
            "Duplicate property values for [" + propertyKey + "].");
      }

      properties.put(propertyKey, propertyValue);
    }

    @Override
    protected void onEndOfFile() {
      // No-op
    }
  }

}
