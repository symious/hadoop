package org.apache.hadoop.security.sdi;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.security.AccessControlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SDICredentialsProviderChain implements SDICredentialsProvider {

  static final Logger LOG = LoggerFactory.getLogger(
      SDICredentialsProviderChain.class);

  private final List<SDICredentialsProvider> credentialsProviders = new LinkedList<>();

  private boolean reuseLastProvider = true;
  private SDICredentialsProvider lastUsedProvider;

  /**
   * Constructs a new SDICredentialsProviderChain with the specified credential providers. When
   * credentials are requested from this provider, it will call each of these credential providers
   * in the same order specified here until one of them returns SDI security credentials.
   *
   * @param credentialsProviders
   *            The chain of credentials providers.
   */
  public SDICredentialsProviderChain(List<? extends SDICredentialsProvider> credentialsProviders) {
    if (credentialsProviders == null || credentialsProviders.size() == 0) {
      throw new IllegalArgumentException("No credential providers specified");
    }
    this.credentialsProviders.addAll(credentialsProviders);
  }

  /**
   * Constructs a new SDICredentialsProviderChain with the specified credential providers. When
   * credentials are requested from this provider, it will call each of these credential providers
   * in the same order specified here until one of them returns SDI security credentials.
   *
   * @param credentialsProviders
   *            The chain of credentials providers.
   */
  public SDICredentialsProviderChain(SDICredentialsProvider... credentialsProviders) {
    if (credentialsProviders == null || credentialsProviders.length == 0) {
      throw new IllegalArgumentException("No credential providers specified");
    }

    this.credentialsProviders.addAll(Arrays.asList(credentialsProviders));
  }

  /**
   * Returns true if this chain will reuse the last successful credentials
   * provider for future credentials requests, otherwise, false if it will
   * search through the chain each time.
   *
   * @return True if this chain will reuse the last successful credentials
   *         provider for future credentials requests.
   */
  public boolean getReuseLastProvider() {
    return reuseLastProvider;
  }

  /**
   * Enables or disables caching of the last successful credentials provider
   * in this chain. Reusing the last successful credentials provider will
   * typically return credentials faster than searching through the chain.
   *
   * @param b
   *            Whether to enable or disable reusing the last successful
   *            credentials provider for future credentials requests instead
   *            of searching through the whole chain.
   */
  public void setReuseLastProvider(boolean b) {
    this.reuseLastProvider = b;
  }

  @Override
  public SDICredentials getCredentials() throws AccessControlException {
    if (reuseLastProvider && lastUsedProvider != null) {
      return lastUsedProvider.getCredentials();
    }

    List<String> exceptionMessages = null;
    for (SDICredentialsProvider provider : credentialsProviders) {
      try {
        SDICredentials credentials = provider.getCredentials();

        if (credentials != null) {
          LOG.debug("Loading credentials from " + provider.toString());

          lastUsedProvider = provider;
          return credentials;
        }
      } catch (Exception e) {
        // Ignore any exceptions and move onto the next provider
        String message = provider + ": " + e.getMessage();
        LOG.debug("Unable to load credentials from " + message);
        if (exceptionMessages == null) {
          exceptionMessages = new LinkedList<String>();
        }
        exceptionMessages.add(message);
      }
    }
    LOG.warn("Unable to load SDI credentials from any provider in the chain: " + exceptionMessages);
    return null;
  }

  @Override
  public void refresh() {
    for (SDICredentialsProvider provider : credentialsProviders) {
      provider.refresh();
    }
  }

}
