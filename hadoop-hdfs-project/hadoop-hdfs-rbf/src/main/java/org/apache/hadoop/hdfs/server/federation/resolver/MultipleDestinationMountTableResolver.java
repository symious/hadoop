/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.federation.resolver;

import java.io.IOException;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.resolver.order.AvailableSpaceResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.order.DestinationOrder;
import org.apache.hadoop.hdfs.server.federation.resolver.order.FixedOrderResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.order.HashFirstResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.order.HashResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.order.LocalResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.order.OrderedResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.order.RandomResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.order.SuffixResolver;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Mount table resolver that supports multiple locations for each mount entry.
 * The returned location contains prioritized remote paths from highest priority
 * to the lowest priority. Multiple locations for a mount point are optional.
 * When multiple locations are specified, both will be checked for the presence
 * of a file and the nameservice for a new file/dir is chosen based on the
 * results of a consistent hashing algorithm.
 * <p>
 * Does the Mount table entry for this path have multiple destinations?
 * <ul>
 * <li>No: Return the location
 * <li>Yes: Return all locations, prioritizing the best guess from the
 * consistent hashing algorithm.
 * </ul>
 * <p>
 * It has multiple options to order the locations: HASH (default), LOCAL,
 * RANDOM, and HASH_ALL.
 * <p>
 * The consistent hashing result is dependent on the number and combination of
 * nameservices that are registered for particular mount point. The order of
 * nameservices/locations in the mount table is not prioritized. Each consistent
 * hash calculation considers only the set of unique nameservices present for
 * the mount table location.
 */
public class MultipleDestinationMountTableResolver extends MountTableResolver {

  private static final Logger LOG =
      LoggerFactory.getLogger(MultipleDestinationMountTableResolver.class);


  /** Resolvers that use a particular order for multiple destinations. */
  private EnumMap<DestinationOrder, OrderedResolver> orderedResolvers =
      new EnumMap<>(DestinationOrder.class);


  public MultipleDestinationMountTableResolver(
      Configuration conf, Router router) {
    super(conf, router);

    // Initialize the ordered resolvers
    addResolver(DestinationOrder.HASH, new HashFirstResolver());
    addResolver(DestinationOrder.LOCAL, new LocalResolver(conf, router));
    addResolver(DestinationOrder.RANDOM, new RandomResolver());
    addResolver(DestinationOrder.HASH_ALL, new HashResolver());
    addResolver(DestinationOrder.SPACE, new AvailableSpaceResolver(conf, router));
    addResolver(DestinationOrder.SUFFIX, new SuffixResolver());
    addResolver(DestinationOrder.FIXED, new FixedOrderResolver());
  }

  @Override
  public PathLocation getDestinationForPath(String path) throws IOException {
    PathLocation mountTableResult = super.getDestinationForPath(path);
    if (mountTableResult == null) {
      LOG.error("The {} cannot find a location for {}",
          super.getClass().getSimpleName(), path);
    } else if (mountTableResult.getDestinationOrder() != null
        && DestinationOrder.SUFFIX.equals(mountTableResult.getDestinationOrder())) {
      mountTableResult = resolveSuffixOrder(path, mountTableResult);
    } else {
      mountTableResult = sortLocation(path, mountTableResult);
    }

    return mountTableResult;
  }

  private PathLocation sortLocation(String path, PathLocation mountTableResult) {
    if (mountTableResult.hasMultipleDestinations()) {
      DestinationOrder order = mountTableResult.getDestinationOrder();
      OrderedResolver orderedResolver = orderedResolvers.get(order);
      if (orderedResolver == null) {
        LOG.error("Cannot find resolver for order {}", order);
      } else {
        String firstNamespace = orderedResolver.getFirstNamespace(path, mountTableResult);
        // Change the order of the name spaces according to the policy
        if (firstNamespace != null) {
          // This is the entity in the tree, we need to create our own copy
          mountTableResult = new PathLocation(mountTableResult, firstNamespace);
          LOG.debug("Ordered locations following {} are {}", order, mountTableResult);
        } else {
          LOG.error("Cannot get main namespace for path {} with order {}", path, order);
        }
      }
    }
    return mountTableResult;
  }

  @VisibleForTesting
  public void addResolver(DestinationOrder order, OrderedResolver resolver) {
    orderedResolvers.put(order, resolver);
  }

  /**
   * Resolves the mount table result for paths in SUFFIX order.
   *
   * For paths with a mount point in SUFFIX order, it retrieves the mount table result based on
   * the suffix path of the given path and creates a new remote location by combining
   * the first level and second level destinations.
   * Example:
   *  If the mount point is /suffix and has destinations ns1->/suffix1 and ns2->/suffix2
   *  If the mount point is /dir1 and has destination ns1->/dir1
   *  When resolving the path /suffix/dir1, it returns the destination ns1->/suffix1/dir1
   *
   * @param path The path to resolve.
   * @param mountTableResult Federated location with multiple destinations.
   * @return New remote location.
   * @throws IOException If it cannot find the location.
   */
  private PathLocation resolveSuffixOrder(String path, PathLocation mountTableResult)
      throws IOException {
    String srcPrefix = mountTableResult.getSourcePath();
    String srcRemainingPath = path.substring(srcPrefix.length());
    if (!StringUtils.isNullOrEmpty(srcRemainingPath)) {
      // Create the second level mount table result for the remaining path.
      PathLocation secondLevelMountTableResult = super.getDestinationForPath(srcRemainingPath);
      if (secondLevelMountTableResult != null) {
        // Check if the second level mount point is in SUFFIX order.
        if (secondLevelMountTableResult.getDestinationOrder().name().
            equals(DestinationOrder.SUFFIX.name())) {
          LOG.error("Cannot find a location for {}, because not support second level mount point" +
              " {} is SUFFIX order.", path, srcRemainingPath);
          throw new RouterResolveException("Cannot find locations for " + path + ", because" +
              " not support second level mount point " + srcRemainingPath + " is SUFFIX order.");
        }

        // Creates a mapping table of second level destination prefixes for each ns.
        Map<String, String> secondLevelPreDestMap = createSecondLevelPreDestMap(mountTableResult,
            srcRemainingPath, path);

        // Create a list of second level destinations.
        List<RemoteLocation> destinations = new LinkedList<>();
        for (RemoteLocation loc: secondLevelMountTableResult.getDestinations()) {
          // If the ns is not found in the secondLevelPreDestMap, default to srcPrefix.
          String dstPrefix = secondLevelPreDestMap.getOrDefault(loc.getNameserviceId(), srcPrefix);
          destinations.add(new RemoteLocation(loc, srcPrefix, dstPrefix));
        }

        mountTableResult = new PathLocation
            (path, destinations, secondLevelMountTableResult.getDestinationOrder());
      }
    }

    return sortLocation(path, mountTableResult);
  }

  /**
   * Creates a mapping table of second level destination prefixes for each ns,
   * the mapping is based on the given first level mount table result and the remaining source path.
   *
   * @param mountTableResult The first level mount table result.
   * @param srcRemainingPath The remaining source path.
   * @param path The path to resolve.
   * @return A mapping table of second level destination prefixes.
   * @throws RouterResolveException
   */
  private Map<String, String> createSecondLevelPreDestMap(PathLocation mountTableResult,
      String srcRemainingPath, String path) throws RouterResolveException {
    // For Example:
    // - Mount point /suffix -> ns1, /suffix1; ns2, /suffix2
    // - When mountTableResult is /suffix/dir -> ns1, /suffix1/dir; ns2, /suffix2/dir
    // - It returns preDestMap: ns1 -> /suffix1; ns2 -> /suffix2
    Map<String, String> preDestMap = new LinkedHashMap();
    List<RemoteLocation> firstLevelRemoteLocations = mountTableResult.getDestinations();
    for (RemoteLocation remoteLocation : firstLevelRemoteLocations) {
      String destPath = remoteLocation.getDest();
      String nsId = remoteLocation.getNameserviceId();
      if (destPath.length() > srcRemainingPath.length()) {
        String preDest = destPath.substring(0, destPath.length() - srcRemainingPath.length());
        preDestMap.put(nsId, preDest);
      } else {
        throw new RouterResolveException("Cannot find locations for " + path + ", because" +
            " Invalid destination path " + destPath + " for ns " + nsId);
      }
    }
    return preDestMap;
  }
}
