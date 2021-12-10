/*
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
package org.apache.hadoop.conf;

import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ThreadUtil;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.hash.MD5FileUtils;
import org.apache.http.conn.util.InetAddressUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_DISTRIBUTED_CONFIG_BASE_PATH;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_DISTRIBUTED_CONFIG_BASE_PATH_DEFAULT;

public class DistributedConfigHelper {

  private static final int DEFAULT_ZK_SESSION_TIMEOUT_IN_MILLIS = 60 * 1000;

  public static final String HDFS = "hdfs";

  public static final String ZK_PATH_SPLITER = "/";

  private static final String COLON = ":";

  private static final String COMMA = ",";

  public static final String MD5_SUFFIX = ".md5";

  public static final Log LOG =
      LogFactory.getLog(DistributedConfigHelper.class);

  public final static String CONFIG_ZK_QUORUM_KEY = "config.zookeeper.quorum";

  public final static String CONFIG_DIR_KEY = "local.config.dir";

  public final static String DISTRIBUTED_CONFIG_ENABLE_KEY =
      "distributed.config.enable";

  private final static String XML_SUFFIX = ".xml";

  public final static String CHARSET = "UTF-8";

  private final static String LOCK_FILE = "cfg.lock";

  private static final List<ACL> ALL_ACL_LIST =
      Arrays.asList(new ACL(ZooDefs.Perms.ALL, new Id("world", "anyone")));

  private static DistributedConfigHelper instance;

  private DistributedConfigHelper() {
  }

  public static DistributedConfigHelper get() {
    // ignore thread unsafe
    if (instance == null) {
      instance = new DistributedConfigHelper();
    }
    return instance;
  }

  /**
   * build configuration.
   *
   * @see #build(String, Configuration, String...)
   * @param nameNodeUri
   * @param configurationClass
   * @param zkQuorum if null,will use local snapshot resource if exists
   * @param snapshotDir
   * @param resources
   */
  public <CONFIG extends Configuration> CONFIG build(String nameNodeUri,
      Class<CONFIG> configurationClass, String zkQuorum, String snapshotDir,
      String... resources) {
    CONFIG configuration = ReflectionUtils.newInstance(
        configurationClass, null);
    configuration.set(DISTRIBUTED_CONFIG_ENABLE_KEY, Boolean.TRUE.toString());
    configuration.set(CONFIG_DIR_KEY, snapshotDir);
    configuration.set(CONFIG_ZK_QUORUM_KEY, zkQuorum);
    return build(nameNodeUri, configuration, resources);
  }

  /**
   * build configuration with zookeeper resource.
   *
   * @param nameNodeUri
   *            namenode uri
   * @param configuration
   *            config instance
   * @param resources
   *            specified resource,support "[all|*]"
   * @return build with specified resources's configuration
   */
  public <CONFIG extends Configuration> CONFIG build(String nameNodeUri,
      CONFIG configuration, String... resources) {
    URI nnUri = null;
    if (StringUtils.isBlank(nameNodeUri) && configuration != null) {
      nnUri = FileSystem.getDefaultUri(configuration);
    } else {
      try {
        nnUri = new URI(nameNodeUri);
      } catch (URISyntaxException e) {
        LOG.error("namenode uri is illegal:" + nameNodeUri, e);
        return configuration;
      }
    }
    return build(nnUri, configuration, resources);
  }

  public synchronized <CONFIG extends Configuration> CONFIG build(
      URI nameNodeUri, CONFIG configuration, String... resources) {
    long start = System.currentTimeMillis();
    String host = null;
    String zkQuorum = null;
    File localConfigDir = null;
    ZooKeeperHolder holder = null;
    boolean isAll = false;
    LocalFileLock fileLock = null;
    Set<String> resourceSet = new HashSet<String>();

    if (configuration == null) {
      LOG.info("configuration is null.");
      return null;
    }

    Configuration localConf = new Configuration();
    if (!Boolean.parseBoolean(getProperty(localConf, DISTRIBUTED_CONFIG_ENABLE_KEY,
        getProperty(configuration, DISTRIBUTED_CONFIG_ENABLE_KEY,
            Boolean.FALSE.toString())))) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("distirbuted config is disable.");
      }
      return configuration;
    }
    if(ArrayUtils.isEmpty(resources)) {
      resources = new String[] {"core-site.xml", "hdfs-site.xml"};
    }

    isAll = filter(Arrays.asList(resources), resourceSet);

    host = nameNodeUri.getHost();
    if (nameNodeUri == null || !HDFS.equals(nameNodeUri.getScheme())
        || host == null) {
      LOG.info("NameNode URI is NULL or host is defective:" + nameNodeUri);
      return configuration;
    }

    if (InetAddressUtils.isIPv4Address(host)) {
      LOG.info("NameNode URI is not HA mode:" + nameNodeUri);
      return configuration;
    }

    zkQuorum = getProperty(localConf, CONFIG_ZK_QUORUM_KEY,
        getProperty(configuration, CONFIG_ZK_QUORUM_KEY, null));

    if (zkQuorum == null && !zkQuorum.isEmpty()) {
      LOG.warn("configuration not spacial zookeeper quorum, "
          + "will use local snapshot resource if not ignore");
    }
    holder = ZooKeeperHolder.init(zkQuorum, localConf);
    String remoteBasePath =
        configuration.get(HADOOP_DISTRIBUTED_CONFIG_BASE_PATH,
            HADOOP_DISTRIBUTED_CONFIG_BASE_PATH_DEFAULT);

    localConfigDir = new File(getProperty(localConf, CONFIG_DIR_KEY, getTmpConfDir()));
    if(!checkLocalPath(localConfigDir)){
      LOG.warn("dir not exists or not have permission:" + localConfigDir);
      return configuration;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("before build configuration:"
          + ReflectionToStringBuilder.toStringExclude(configuration,
          new String[] {"updatingResource"}));
    }

    // Get remote resource md5
    Set<RemoteResource> remoteResourceSet = getRemoteResource(holder, resourceSet,
        remoteBasePath);

    // Get Read lock and check md5
    fileLock = new LocalFileLock(new File(localConfigDir, LOCK_FILE), true);
    try {
      fileLock.lock();

      Set<String> toRemoveResource = new HashSet<>(resourceSet);
      Set<RemoteResource> toUpdateResource = new HashSet<>();
      for (RemoteResource remoteResource : remoteResourceSet) {
        File file = new File(localConfigDir, remoteResource.md5Name);
        toRemoveResource.remove(remoteResource.fileName);
        if (file.exists() &&
            read(file).equals(new String(remoteResource.md5Content))) {
          continue;
        }
        toUpdateResource.add(remoteResource);
      }

      // Download files from Remote
      if (!toUpdateResource.isEmpty()) {
        fileLock.upgradeLock();
        Set<String> files = getFileName(toUpdateResource);
        Map<String, byte[]> storeResources =
            downloadZKResource(holder, remoteBasePath, files, false);
        storeLocalResource(localConfigDir, storeResources);
      }

      // Remove resources
      if (!toRemoveResource.isEmpty()) {
        fileLock.upgradeLock();
        for (String fileName : toRemoveResource) {
          new File(localConfigDir, fileName).delete();
          new File(localConfigDir, fileName + MD5_SUFFIX).delete();
        }
      }

      Map<String, InputStream> inputStreams = getResourceStream(
          localConfigDir, isAll, resourceSet);
      if (MapUtils.isNotEmpty(inputStreams)) {
        for (Map.Entry<String, InputStream> entry : inputStreams.entrySet()) {
          configuration.addResource(entry.getValue(),
              "distributed." + entry.getKey());
        }
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (KeeperException e) {
      e.printStackTrace();
    } finally {
      fileLock.unlock();
      if (holder != null) {
        holder.close();
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("build configuration finished,cost:" +
          (System.currentTimeMillis() - start) + " ms");
      LOG.debug("after build configuration:"
          + ReflectionToStringBuilder.toStringExclude(configuration,
          new String[] {"updatingResource"}));
      dump(localConfigDir.getParentFile(), configuration);
    }

    return configuration;
  }

  private String getTmpConfDir() {
    return File.separator + "tmp"
        + File.separator + System.getProperty("user.name")
        + File.separator + "dist-config";
  }

  private Map<String, InputStream> getResourceStream(
      File localConfigDir, boolean isAll,
      final Set<String> resourceSet) {
    // process extra situation
    Map<String, InputStream> inputStreams = null;
    Set<File> localConfigFiles =
        getLocalFile(localConfigDir, resourceSet, isAll);
    if (!isAll) {
      Set<String> missing = Sets.difference(resourceSet,
          localConfigFiles);
      if (CollectionUtils.isNotEmpty(missing) && LOG.isDebugEnabled()) {
        LOG.info("Fetch resources:" + resourceSet
            + ", load resources:" + localConfigFiles + ", missing "
            + missing);
      }
    }
    inputStreams = loadLocalResource(localConfigFiles);
    return inputStreams;
  }

  private String getProperty(Configuration configuration, String key,
      String defaultValue) {
    String value = "";
    if (configuration != null) {
      value = configuration.get(key);
    }
    if (StringUtils.isBlank(value)) {
      value = System.getProperty(key);
      return StringUtils.isNotBlank(value) ? value : defaultValue;
    }
    return value;
  }

  public boolean uploadResource(Configuration config, String... resources) {
    String quorum = config.get(CONFIG_ZK_QUORUM_KEY);
    String dir = config.get(CONFIG_DIR_KEY);
    if (quorum == null || dir == null) {
      LOG.warn("config is defective,"
          + CONFIG_ZK_QUORUM_KEY + ":" + quorum + "," + CONFIG_DIR_KEY
          + ":" + dir);
      return false;
    }
    return uploadResource(quorum, dir, config, resources);
  }

  public boolean dropResource(Configuration config, String... resources) {
    if (resources.length == 0) {
      LOG.warn("Drop is dangerous, please specify resource files!");
      return false;
    }
    String quorum = config.get(CONFIG_ZK_QUORUM_KEY);
    if (quorum == null) {
      LOG.warn("Config is defective," + CONFIG_ZK_QUORUM_KEY + "is null.");
      return false;
    }
    return dropResource(quorum, config, resources);
  }

  /**
   * upload specified resource to remote.
   *
   * @param zkQuorum
   * @param srcDir
   * @param resources
   * @throws Exception
   */
  public boolean uploadResource(String zkQuorum, String srcDir,
      Configuration config, final String... resources) {
    long start = System.currentTimeMillis();
    ZooKeeperHolder holder = null;

    holder = ZooKeeperHolder.init(zkQuorum, config);

    try {
      if (!holder.connect()) {
        LOG.warn("can't connect zk,querom:" + zkQuorum);
        return false;
      }

      LOG.info("create zookeeper,quorum:" + zkQuorum);
      final long uploadTimestamp = System.currentTimeMillis();

      File srcConfigDir = new File(srcDir);
      if (!srcConfigDir.isDirectory() || !srcConfigDir.canRead()) {
        String message = "Source dir can't read:" + srcConfigDir;
        LOG.error(message);
        return false;
      }

      final Set<String> rsSet = new HashSet<String>();

      final boolean isAll = filter(resources != null ? Arrays.asList(resources)
          : null, rsSet);

      Set<File> rsFilesSet= getLocalFile(srcConfigDir, rsSet, isAll);

      if (rsFilesSet.isEmpty()
          || (!isAll && rsFilesSet.size() != rsSet.size())) {
        LOG.error("Can not found resource files, parent dir:" + srcDir
            + (isAll ? " " : ", missing:"
            + Sets.difference(rsSet, rsFilesSet)));
        return false;
      }

      LOG.info("found resources:" + rsFilesSet);

      String remoteBasePath = config.get(HADOOP_DISTRIBUTED_CONFIG_BASE_PATH,
          HADOOP_DISTRIBUTED_CONFIG_BASE_PATH_DEFAULT);

      boolean exists = false;

      if (holder.get().exists(remoteBasePath, true) == null) {
        holder.get().create(remoteBasePath, null, ALL_ACL_LIST,
            CreateMode.PERSISTENT);
        LOG.info("Path not exists :" + remoteBasePath
            + " on ZK, create it first.");
      }

      exists = (holder.get().exists(remoteBasePath, true) != null);
      assert exists;

      createOrUpdateZKResource(holder, remoteBasePath, rsFilesSet);

      LOG.info("upload finished,cost:"
          + (System.currentTimeMillis() - start) + " ms");

    } catch (Exception e) {
      LOG.error("connect zookeeper occur exception, quorum:" + zkQuorum, e);
      return false;
    } finally {
      holder.close();
    }
    return true;
  }

  /**
   * drop remote resource.
   *
   * @param zkQuorum
   * @param resources
   * @throws Exception
   */
  public boolean dropResource(String zkQuorum, Configuration config,
      final String... resources) {
    long start = System.currentTimeMillis();

    ZooKeeperHolder holder = null;
    holder = ZooKeeperHolder.init(zkQuorum, config);
    try {
      if (!holder.connect()) {
        LOG.warn("can't connect ZK:" + zkQuorum);
        return false;
      }
      final Set<String> rsSet = new HashSet<>();

      final boolean dropAll = filter(Arrays.asList(resources), rsSet);

      String remoteBasePath = config.get(HADOOP_DISTRIBUTED_CONFIG_BASE_PATH,
          HADOOP_DISTRIBUTED_CONFIG_BASE_PATH_DEFAULT);
      final String basePath = remoteBasePath;

      if (holder.get().exists(basePath, true) == null) {
        LOG.info("Remote path not exists:" + basePath);
        return false;
      }

      dropZKResource(holder, basePath, rsSet, dropAll);

    } catch (Exception e) {
      LOG.error("Got exception connecting to zookeeper: " + zkQuorum, e);
      return false;
    } finally {
      holder.close();
    }
    LOG.info("Drop resource finished,cost:"
        + (System.currentTimeMillis() - start) + " ms");
    return true;
  }

  private static boolean filter(List<String> resources,
      Set<String> resourceSet) {
    if (CollectionUtils.isEmpty(resources)) {
      resourceSet.add("core-site.xml");
      resourceSet.add("hdfs-site.xml");
      return false;
    }

    for (String resource : resources) {
      if (resource.equals("*") || resource.equalsIgnoreCase("ALL")) {
        return true;
      }
      resourceSet.add(resource.toLowerCase());
    }
    return false;
  }

  private Map<String, InputStream> loadLocalResource(
      Collection<File> localConfigFiles) {
    Map<String, InputStream> inputStreams =
        new LinkedHashMap<String, InputStream>();
    for (File localFile : localConfigFiles) {
      String xml = read(localFile);
      if (StringUtils.isNotBlank(xml)) {
        inputStreams.put(localFile.getName(),
            new ByteArrayInputStream(xml.getBytes()));
        LOG.info("fetch local resource:" + localFile.getName());
      }
    }
    return inputStreams;
  }

  public boolean listResource(Configuration config) {
    String quorum = config.get(CONFIG_ZK_QUORUM_KEY);
    if (quorum == null) {
      LOG.warn("config is defective," + CONFIG_ZK_QUORUM_KEY + ":" + quorum);
      return false;
    }

    return listResource(quorum, config);
  }

  private boolean listResource(String quorum, Configuration config) {
    long start = System.currentTimeMillis();
    ZooKeeperHolder holder = null;

    holder = ZooKeeperHolder.init(quorum, config);
    try {
      if (!holder.connect()) {
        LOG.warn("can't connect ZK: " + quorum);
        return false;
      }

      String remoteBasePath = config.get(HADOOP_DISTRIBUTED_CONFIG_BASE_PATH,
          HADOOP_DISTRIBUTED_CONFIG_BASE_PATH_DEFAULT);
      final String basePath = remoteBasePath;

      if (holder.get().exists(basePath, true) == null) {
        LOG.error("Path not exists:" + basePath);
        return false;
      }

      List<String> listResources = listZKResource(holder, basePath);
      System.out.println("Remote resource: " + listResources);
      LOG.info("download resource finished,cost:"
          + (System.currentTimeMillis() - start) + " ms");
    } catch (Exception e) {
      LOG.error("Got exception, zk quorum:" + quorum, e);
      return false;
    } finally {
      holder.close();
    }
    return true;
  }

  public boolean downloadResource(Configuration config, String... specifiedRs) {
    String quorum = config.get(CONFIG_ZK_QUORUM_KEY);
    String dir = config.get(CONFIG_DIR_KEY);
    if (quorum == null || dir == null) {
      LOG.warn("config is defective,"
          + CONFIG_ZK_QUORUM_KEY + ":" + quorum + "," + CONFIG_DIR_KEY
          + ":" + dir);
      return false;
    }

    return downloadResource(quorum, dir, specifiedRs, config);
  }

  private boolean downloadResource(String quorum, String dir,
      String[] specifiedRs, Configuration config) {
    long start = System.currentTimeMillis();
    ZooKeeperHolder holder = null;

    holder = ZooKeeperHolder.init(quorum, config);
    try {
      if (!holder.connect()) {
        LOG.warn("can't connect ZK: " + quorum);
        return false;
      }

      File dest = new File(dir);
      if (!checkLocalPath(dest)) {
        LOG.warn("Dest not exists:" + dir);
        return false;
      }

      final Set<String> rsSet = new HashSet<String>();

      final boolean downloadAll = filter(Arrays.asList(specifiedRs), rsSet);
      String remoteBasePath = config.get(HADOOP_DISTRIBUTED_CONFIG_BASE_PATH,
          HADOOP_DISTRIBUTED_CONFIG_BASE_PATH_DEFAULT);
      final String basePath = remoteBasePath;

      if (holder.get().exists(basePath, true) == null) {
        LOG.info("not existed data on the path:" + basePath);
        return false;
      }

      Map<String, byte[]> storeResources =
          downloadZKResource(holder, basePath, rsSet, downloadAll);
      storeLocalResource(dest, storeResources);
      LOG.info("download resource finished,cost:"
          + (System.currentTimeMillis() - start) + " ms");
    } catch (Exception e) {
      LOG.error("Got exception, zk quorum:" + quorum, e);
      return false;
    } finally {
      holder.close();
    }
    return true;
  }

  public boolean dumpResource(Configuration config, String... specifiedRs) {
    String quorum = config.get(CONFIG_ZK_QUORUM_KEY);
    String dir = config.get(CONFIG_DIR_KEY);
    if (quorum == null || dir == null) {
      LOG.warn("config is defective,"
          + CONFIG_ZK_QUORUM_KEY + ":" + quorum + "," + CONFIG_DIR_KEY
          + ":" + dir);
      return false;
    }

    return dumpResource(quorum, dir, config, specifiedRs);
  }

  public boolean dumpResource(String quorum, String dir, Configuration config,
      String... specifiedRs) {
    long start = System.currentTimeMillis();
    ZooKeeperHolder holder = null;
    holder = ZooKeeperHolder.init(quorum, config);
    try {
      if (!holder.connect()) {
        LOG.warn("can't connect ZK:" + quorum);
        return false;
      }

      File dest = new File(dir);
      if (!checkLocalPath(dest)) {
        LOG.warn("Dest not exists:" + dir);
        return false;
      }

      final Set<String> rsSet = new HashSet<String>();

      final boolean dumpAll = filter(Arrays.asList(specifiedRs), rsSet);
      final String basePath = config.get(HADOOP_DISTRIBUTED_CONFIG_BASE_PATH,
          HADOOP_DISTRIBUTED_CONFIG_BASE_PATH_DEFAULT);

      if (holder.get().exists(basePath, true) == null) {
        LOG.info("Remote dir empty:" + basePath);
        return false;
      }

      Configuration conf = dumpZKResource(holder, basePath, rsSet, dumpAll);

      dump(dest, conf);
      LOG.info("dump resource finished,cost:"
          + (System.currentTimeMillis() - start) + " ms");
    } catch (Exception e) {
      LOG.error("Got exception, zk quorum:" + quorum, e);
      return false;
    } finally {
      holder.close();
    }
    return true;
  }

  /**
   * Get all configured name services.
   */
  public static List<String> getAllNameServices(Configuration config) {
    String quorum = config.get(CONFIG_ZK_QUORUM_KEY);
    ZooKeeperHolder holder =holder = ZooKeeperHolder.init(quorum, config);
    List<String> ret = new ArrayList<String>();
    try {
      if (!holder.connect()) {
        LOG.warn("can't connect zk,zk's quorum:" + quorum);
        return ret;
      }
      ret = holder.get().getChildren(config.get(HADOOP_DISTRIBUTED_CONFIG_BASE_PATH,
          HADOOP_DISTRIBUTED_CONFIG_BASE_PATH_DEFAULT), true);
    } catch (Exception e) {
      LOG.error("occur exception, zk quorum:" + quorum, e);
    } finally {
      holder.close();
    }
    return ret;
  }

  private void dump(File dest, Configuration conf) {
    FileOutputStream out = null;
    try {
      out = new FileOutputStream(new File(dest, "dump.xml"));
      conf.writeXml(out);
    } catch (Exception e) {
      LOG.error("dump conf error,dest:" + dest.getPath(), e);
    } finally {
      IOUtils.closeStream(out);
    }
  }

  private void storeLocalResource(File localPath, Map<String,
      byte[]> needStoreResources) {
    OutputStream output = null;
    File storeFile;
    for (Map.Entry<String, byte[]> entry : needStoreResources.entrySet()) {
      storeFile = new File(localPath, entry.getKey());
      try {
        if (storeFile.createNewFile()) {
          trySetXWRPermission(storeFile);
        }
        output = new FileOutputStream(storeFile);
        output.write(entry.getValue());
        output.flush();
        LOG.info("store resource:" + storeFile.getName());
      } catch (IOException e) {
        LOG.error("store local resource file failed,parent path:" + localPath
            + ",resource:" + storeFile.getName(), e);
        return;
      } finally {
        IOUtils.closeStream(output);
      }
    }
  }

  private static String read(File versionFile) {
    StringWriter writer = new StringWriter();
    char[] cbuf = new char[512];
    int idx = -1;
    Reader reader = null;
    try {
      reader = new InputStreamReader(new FileInputStream(versionFile));
      while ((idx = reader.read(cbuf)) != -1) {
        writer.write(cbuf, 0, idx);
      }
    } catch (IOException e) {
      LOG.error("unable load local config file version,file:"
          + versionFile.getName(), e);
    } finally {
      IOUtils.closeStream(reader);
    }

    String versionStr = writer.getBuffer().toString();
    return versionStr;
  }

  private boolean checkLocalPath(File localConfigDir) {
    return checkLocalPath(localConfigDir, true);
  }

  private boolean checkLocalPath(File localConfigDir, boolean createIfAbsent) {
    try {
      if (!localConfigDir.exists()) {
        if (createIfAbsent && localConfigDir.mkdirs()) {
          trySetXWRPermission(localConfigDir);
          return true;
        }
        return false;
      }
      return localConfigDir.isDirectory() && localConfigDir.canRead()
          && localConfigDir.canWrite();
    } catch (Exception e) {
      LOG.error("check local config dir faild,dir:" + localConfigDir, e);
      return false;
    }
  }

  private boolean trySetXWRPermission(File file) {
    if(file == null || !file.exists()){
      return false;
    }
    try {
      file.setExecutable(true, false);
      file.setReadable(true, false);
      file.setWritable(true, false);
    } catch(Exception e) {
      LOG.error("set xwr permission failed", e);
      return false;
    }
    return true;
  }

  private Set<File> getLocalFile(File localConfigDir,
      final Collection<String> resources, final boolean isAll) {
    if (!localConfigDir.exists() || !localConfigDir.isDirectory()) {
      return Collections.emptySet();
    }

    final Set<File> resourceSet = new HashSet<>();

    File[] configFiles = localConfigDir.listFiles(new FileFilter() {
      @Override
      public boolean accept(File file) {
        String fileName = file.getName().toLowerCase();
        int idx = fileName.indexOf(XML_SUFFIX);
        if (file.isFile() && (idx != -1)) {
          if (isAll) {
            return true;
          }
          if (resources.contains(fileName)) {
            return true;
          }
          return false;
        }
        return false;
      }
    });

    if (configFiles != null) {
      for (File configFile : configFiles) {
        resourceSet.add(configFile);
      }
    }

    return resourceSet;
  }

  /**
   * thread safe mode,reconnect if session expired.
   */
  public static final class ZooKeeperHolder {
    private ZooKeeperHolder(String quorum, Configuration configuration) {
      this.quorum = quorum;
      this.sessionTimeout =
          configuration.getInt("config.zk.session.timeout",
              DEFAULT_ZK_SESSION_TIMEOUT_IN_MILLIS);
      this.maxRetryTime =
          configuration.getInt("config.zk.connect.max.retry.times", 16);
    }

    private int maxRetryTime = 16;

    private long waitTime = 500L;

    private volatile ZooKeeper zooKeeper;

    private String quorum;

    private int sessionTimeout;

    private Object mutex = new Object();

    private volatile boolean initialized = false;

    private volatile boolean isTimeout = false;

    public static ZooKeeperHolder init(String quorum, Configuration conf) {
      ZooKeeperHolder holder = new ZooKeeperHolder(quorum, conf);
      return holder;
    }

    private void build() {
      try {
        this.zooKeeper = new ZooKeeper(quorum, sessionTimeout,
            this.new DummyWatcher());
      } catch (IOException e) {
        LOG.error("connect zk error.", e);
        this.initialized = true;
      }
      isTimeout = false;
    }

    public boolean connect() {
      return get() != null && get().getState().isConnected();
    }

    public ZooKeeper get() {
      int count = 0;
      if (!initialized) {
        build();
      }
      long waitT = this.waitTime;
      if (zooKeeper != null) {
        while (!initialized || (isTimeout && initialized)) {
          long time = System.currentTimeMillis();
          synchronized (mutex) {
            try {
              mutex.wait(Math.min(waitT, 30000L));
            } catch (InterruptedException e) {
              LOG.warn("get zookeeper is interrupted.", e);
              initialized = true;
              isTimeout = false;
            }
          }
          long actTime = System.currentTimeMillis() - time;
          if (LOG.isDebugEnabled()) {
            LOG.debug("connect zk  cost  " + actTime
                + "ms, and wait count:" + count);
          }
          waitT += actTime;

          if (++count >= maxRetryTime) {
            LOG.warn("get zookeeper over time,retry:" + this.maxRetryTime);
            initialized = true;
            break;
          }
        }
      }

      return zooKeeper;
    }

    public void close() {
      if (zooKeeper != null) {
        try {
          get().close();
        } catch (Exception e) {
          LOG.error("close zookeeper failed.", e);
        }
      }
      initialized = false;
    }

    /**
     * Dummy Watcher.
     */
    public class DummyWatcher implements Watcher {
      @Override
      public void process(WatchedEvent event) {
        if (KeeperState.SyncConnected == event.getState()) {
          initialized = true;
          synchronized (mutex) {
            mutex.notify();
          }
        }

        if (KeeperState.Expired == event.getState()) {
          isTimeout = true;
          close();
          build();
          synchronized (mutex) {
            mutex.notify();
          }
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("zk event:" + event);
        }
      }
    }

    public void setExtra(int maxRetry, long waitT) {
      this.maxRetryTime = maxRetry;
      this.waitTime = waitT;
    }
  }

  public static void main(String[] args) throws Exception {

    Tool tool = new DistributedConfigTool();
    int res = ToolRunner.run(tool, args);
    System.exit(res);
  }

  /**
   * File Lock.
   */
  public class LocalFileLock {

    private RandomAccessFile raf;
    private File lockFile;
    private boolean shared = true;
    private FileLock fileLock;
    private boolean locked = false;
    private boolean supported = true;
    private boolean initialized = false;

    LocalFileLock(File lockFile, boolean shared) {
      init(lockFile, shared);
    }

    private void init(File lockF, boolean share) {
      this.lockFile = lockF;
      if (supported) {
        if (!lockFile.exists()) {
          try {
            if(lockFile.createNewFile()){
              trySetXWRPermission(lockFile);
            }else{
              supported = false;
            }
          } catch (IOException e) {
            LOG.error("create lock file error,file name:"
                + lockFile.getName(), e);
            supported = false;
          }
        }
      }

      if (supported) {
        try {
          this.raf = new RandomAccessFile(lockFile, "rws");
        } catch (FileNotFoundException e) {
          LOG.error("not found file,file name:" + lockFile.getName(), e);
          supported = false;
        }
      }
      this.shared = share;
      this.locked = false;
      this.initialized = true;
      LOG.debug("support file lock:" + supported);
    }

    public void lock() {
      if (supported) {
        int times = 0;
        while (!locked && times++ < 5) {
          try {
            fileLock = raf.getChannel().lock(0, Long.MAX_VALUE, shared);
          } catch (OverlappingFileLockException e) {
            // multiple thread lock conflict
            ThreadUtil.sleepAtLeastIgnoreInterrupts(500L);
            locked = false;
            continue;
          } catch (IOException e) {
            LOG.error(" lock file error,file name:" + lockFile.getName(), e);
          }
          locked = true;
        }
      }

      supported = locked;
    }

    public void unlock() {
      if (fileLock != null) {
        try {
          fileLock.release();
        } catch (IOException e) {
          LOG.error(" lock file error,file name:" + lockFile.getName(), e);
        }
      }
      close();
    }

    private void close() {
      IOUtils.closeStream(raf);
      raf = null;
      locked = false;
      initialized = false;
      supported = true;
    }

    public void upgradeLock() {
      opLock(true);
    }

    public void demoteLock() {
      opLock(false);
    }

    private void opLock(boolean upgrade) {
      if (!supported) {
        return;
      }
      boolean share = !upgrade;
      if (fileLock == null) {
        this.shared = share;
      }

      if (locked) {
        // not need op
        if (this.shared == share) {
          return;
        }
        unlock();
      }
      if (!initialized) {
        init(lockFile, shared);
      }
      lock();
    }
  }

  /**
   * Operation.
   */
  public enum OperationEnum {
    UPLOAD, DOWNLOAD, DROP, DUMP, LIST;
  }

  /**
   * Tool.
   */
  public static class DistributedConfigTool implements Tool {

    private Configuration conf;

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
    }

    @Override
    public Configuration getConf() {
      return this.conf;
    }

    @Override
    public int run(String[] args) throws Exception {
      DistributedConfigHelper helper = DistributedConfigHelper.get();
      boolean needHelp = false;
      OperationEnum op = null;
      if (args == null || args.length < 1 || !args[0].startsWith("-")) {
        needHelp = true;
      }

      boolean success = false;
      if (!needHelp) {
        op = OperationEnum.valueOf(args[0].substring(1).toUpperCase());
        if (op != null) {
          switch (op) {
            case UPLOAD:
            case DOWNLOAD:
            case DUMP:
              if (args.length < 2) {
                needHelp = true;
              }
              break;
            case DROP:
              if (args.length < 1) {
                needHelp = true;
              }
              break;
            case LIST:
              break;
            default:
              needHelp = true;
              break;
          }
        }
      }

      if (!needHelp && op != null) {
        String[] specifiedRs = new String[] {"*"};

        switch (op) {
          case UPLOAD:
            if (args.length > 2) {
              specifiedRs = new String[args.length - 2];
              System.arraycopy(args, 2, specifiedRs, 0, specifiedRs.length);
            }
            conf.set(CONFIG_DIR_KEY, args[1]);
            success = helper.uploadResource(conf, specifiedRs);
            break;
          case DROP:
            if (args.length > 1) {
              specifiedRs = new String[args.length - 1];
              System.arraycopy(args, 1, specifiedRs, 0, specifiedRs.length);
            }
            success = helper.dropResource(conf, specifiedRs);
            break;
          case DUMP:
            if (args.length > 2) {
              specifiedRs = new String[args.length - 2];
              System.arraycopy(args, 2, specifiedRs, 0, specifiedRs.length);
            }
            conf.set(CONFIG_DIR_KEY, args[1]);
            success = helper.dumpResource(conf, specifiedRs);
            break;
          case DOWNLOAD:
            if (args.length > 2) {
              specifiedRs = new String[args.length - 2];
              System.arraycopy(args, 2, specifiedRs, 0, specifiedRs.length);
            }
            conf.set(CONFIG_DIR_KEY, args[1]);
            success = helper.downloadResource(conf, specifiedRs);
            break;
          case LIST:
            success = helper.listResource(conf);
            break;
          default:
            break;
        }
        System.out.println(op.toString().toLowerCase()
            + (success ? " successfully" : " faild"));
      }

      if (needHelp) {
        if (op != null) {
          switch (op) {
            case UPLOAD:
              System.out.println("usage:[-upload <confdir> <resource>...]");
              break;
            case DROP:
              System.out.println("usage:[-drop <resource>...]");
              break;
            case DUMP:
              System.out.println("usage:[-dump <dest> <resource>...]");
              break;
            case DOWNLOAD:
              System.out.println("usage:[-download <dest> <resource>...]");
              break;
            case LIST:
              System.out.println("usage:[-list]");
              break;
            default:
              break;
          }
        } else {
          System.out.println("usage:[-upload|-download|-drop|-dump|-list]");
        }
      }
      return -1;
    }
  }

  private static void createOrUpdateZKResource(ZooKeeperHolder holder,
      String basePath, Set<File> rsFileSet)
      throws InterruptedException, KeeperException, IOException {
    List<String> children = holder.get().getChildren(basePath, true);

    for (File rsFile : rsFileSet) {
      String fileName = rsFile.getName();
      String md5Name = rsFile.getName() + MD5_SUFFIX;
      String zkFilePath = basePath + ZK_PATH_SPLITER + fileName;
      String zkMd5Path = basePath + ZK_PATH_SPLITER + md5Name;

      if (children.contains(fileName)) {
        holder.get().setData(zkFilePath,
            read(rsFile).getBytes(), -1);
        LOG.info("Upload resource file: " + fileName);
      } else {
        holder.get().create(zkFilePath,
            read(rsFile).getBytes(),
            ALL_ACL_LIST, CreateMode.PERSISTENT);
        LOG.info("Create resource file: " + fileName);
      }

      if (children.contains(md5Name)) {
        holder.get().setData(zkMd5Path,
            MD5FileUtils.computeMd5ForFile(rsFile).toString().getBytes(),
            -1);
        LOG.info("Update resource MD5 file: " + md5Name);
      } else {
        holder.get().create(zkMd5Path,
            MD5FileUtils.computeMd5ForFile(rsFile).toString().getBytes(),
            ALL_ACL_LIST, CreateMode.PERSISTENT);
        LOG.info("Create resource MD5 file: " + md5Name);
      }
    }
  }

  private static void dropZKResource(ZooKeeperHolder holder, String basePath,
      Set<String> rsFileSet, boolean dropAll)
      throws InterruptedException, KeeperException {
    List<String> childrenShortName = holder.get().getChildren(basePath, true);

    if (dropAll) {
      for (String childName : childrenShortName) {
        holder.get().delete(basePath + ZK_PATH_SPLITER + childName, -1);
      }
    } else {
      String toDropFile, toDropMd5;
      for (String rsFile : rsFileSet) {
        toDropFile = rsFile;
        toDropMd5 = toDropFile + MD5_SUFFIX;
        if (childrenShortName.contains(toDropFile)) {
          holder.get().delete(basePath + ZK_PATH_SPLITER + toDropFile, -1);
        }
        if (childrenShortName.contains(toDropMd5)) {
          holder.get().delete(basePath + ZK_PATH_SPLITER + toDropMd5, -1);
        }
      }
    }
  }

  private static Configuration dumpZKResource(ZooKeeperHolder holder,
      String basePath, Set<String> rsFileSet, boolean dumpAll)
      throws InterruptedException, KeeperException {
    Configuration baseConf = new Configuration(false);
    List<String> childrenShortName = holder.get().getChildren(basePath, true);

    if (dumpAll) {
      for (String childName : childrenShortName) {
        if (!childName.endsWith(MD5_SUFFIX)) {
          byte[] data = holder.get().getData(basePath + ZK_PATH_SPLITER + childName,
              true, null);
          ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
          baseConf.addResource(inputStream, "remote-" + childName);
        }
      }
      LOG.debug("Dumped all ZK resources: " + childrenShortName);
    } else {
      for (String rsFile : rsFileSet) {
        if (childrenShortName.contains(rsFile)) {
          byte[] data =
              holder.get().getData(basePath + ZK_PATH_SPLITER + rsFile,
                  true, null);
          ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
          baseConf.addResource(inputStream, "remote-" + rsFile);
          LOG.debug("Dumped ZK resources: " + rsFile);
        } else {
          LOG.debug("Skip Dump ZK resources: " + rsFile);
        }
      }
    }
    return baseConf;
  }

  private static Map<String, byte[]> downloadZKResource(ZooKeeperHolder holder,
      String basePath, Set<String> rsFileSet, boolean downloadAll)
      throws InterruptedException, KeeperException {
    Map<String, byte[]> zkResources = new HashMap<>();
    List<String> childrenShortName = holder.get().getChildren(basePath, true);

    if (downloadAll) {
      for (String childName : childrenShortName) {
        byte[] data = holder.get().getData(basePath + ZK_PATH_SPLITER + childName,
            true, null);
        zkResources.put(childName, data);
      }
    } else {
      String md5File;
      for (String rsFile : rsFileSet) {
        md5File = rsFile + MD5_SUFFIX;
        if (childrenShortName.contains(rsFile)) {
          byte[] data = holder.get().getData(basePath + ZK_PATH_SPLITER + rsFile,
              true, null);
          zkResources.put(rsFile, data);
        }
        if (childrenShortName.contains(md5File)) {
          byte[] data = holder.get().getData(basePath + ZK_PATH_SPLITER + md5File,
              true, null);
          zkResources.put(md5File, data);
        }
      }
    }
    return zkResources;
  }

  private static List<String> listZKResource(ZooKeeperHolder holder,
      String basePath) throws InterruptedException, KeeperException {
    List<String> childrenShortName = holder.get().getChildren(basePath, true);
    return childrenShortName;
  }
  private Set<RemoteResource> getRemoteResource(ZooKeeperHolder holder,
      Set<String> resourceSet, String basePath) {
    Set<RemoteResource> result = new HashSet<>();
    for (String resource : resourceSet) {
      String md5Name = resource + MD5_SUFFIX;
      byte[] md5Content = getRemoteResourceContent(holder, md5Name, basePath);
      if (md5Content != null) {
        result.add(new RemoteResource(resource, md5Name, md5Content));
      }
    }
    return result;
  }

  private static byte[] getRemoteResourceContent(ZooKeeperHolder holder,
      String resourceName, String basePath) {
    byte[] result = null;
    try {
      if (holder.get().exists(basePath + ZK_PATH_SPLITER + resourceName, true) != null) {
        result = holder.get()
            .getData(basePath + ZK_PATH_SPLITER + resourceName, true, null);
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (KeeperException e) {
      e.printStackTrace();
    }
    return result;
  }

  public static Set<String> getFileName(Set<RemoteResource> remoteResourceSet) {
    Set<String> files = new HashSet<>();
    for (RemoteResource resource : remoteResourceSet) {
      files.add(resource.fileName);
    }
    return files;
  }

  private class RemoteResource {
    private String fileName;
    private String md5Name;
    private byte[] md5Content;
    public RemoteResource(String fileName, String md5Name, byte[] md5Content) {
      this.fileName = fileName;
      this.md5Name = md5Name;
      this.md5Content = md5Content;
    }
  }
}