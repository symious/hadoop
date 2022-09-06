package org.apache.hadoop.hdfs.server.zoneservice.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;

import java.net.URI;
import java.util.Collection;

public class ZoneServiceUtil {
  public static URI getNamespaceUri(String namespace, Configuration conf)
      throws IllegalArgumentException {
    Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
    for (URI namenode: namenodes) {
      if (namenode.getAuthority().equals(namespace)) {
        return namenode;
      }
    }
    throw new IllegalArgumentException(
        "Cannot find the NameNode for namespace: " + namespace);
  }
}
