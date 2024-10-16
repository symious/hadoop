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
package org.apache.hadoop.hdfs.server.zoneservice;

import org.apache.commons.io.Charsets;
import org.apache.hadoop.thirdparty.com.google.common.collect.Sets;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.server.zoneservice.web.resources.ResultCode;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;

import static org.apache.hadoop.util.Time.now;

/**
 * The class is supports automatically generate replication rules
 * from kakfa record when heavy sudden traffic happens.
 */
public class ReplicationRuleGenerateKafkaTrigger {

  private static final Logger LOG = LoggerFactory.getLogger(
      ReplicationRuleGenerateKafkaTrigger.class);

  private Consumer<String, String> consumer;
  private final DistributedFileSystem fs;
  private String ruleGenerateKey;
  private long pathSizeLimit;
  private long minCrossReadSize;
  private int pollTimeOut;
  private long capacityLimit;
  private int maxRateLimit;
  // Whether to enable the operation of migrating the replication of dc.
  private boolean supportMigrateReplica = false;
  private Set<String> validDataCenters;
  // "," is the separator of pattern "/dc1:replica1,/dc2:replica2"
  private final static String SECTION_SEPARATOR = ",";
  private final static String FIELD_SEPARATOR = ":";
  private final ReplicationRuleManager replicationRuleManager;
  private final Thread monitorServer;
  private final ExecutorService executor;
  private final Set<String> filterPaths = Collections.synchronizedSet(new HashSet<String>());
  // paths in this set will be skipped from processing.
  private Set<String> blacklistPaths = new HashSet<>();
  private Semaphore rateLimiter;
  private final Configuration conf;

  public ReplicationRuleGenerateKafkaTrigger(Configuration conf) throws IOException {
    this.conf = conf;
    this.fs = (DistributedFileSystem) FileSystem.get(conf);
    this.replicationRuleManager = new ReplicationRuleManager(conf);
    setReplicationRuleParam(conf);
    initKafka();
    this.rateLimiter = new Semaphore(maxRateLimit);
    executor = HadoopExecutors.newCachedThreadPool(
        new ThreadFactoryBuilder()
            .setNameFormat("addReplicationRule #%d")
            .build());
    this.monitorServer = new Thread(new Monitor(), "replicationRuleGenerateKafkaTrigger");
    this.monitorServer.start();
  }

  public void shutdown() {
    if (consumer != null) {
      consumer.commitSync();
      consumer.close();
    }
    if (monitorServer != null) {
      monitorServer.interrupt();
    }
    if (executor != null) {
      executor.shutdown();
    }
  }

  private void initKafka() {
    LOG.info("Init kafka consumer.");
    final String username =
        conf.getTrimmed(DFSConfigKeys.DFS_ZONE_GENERATE_REPLICATION_RULE_KAFKA_USERNAME);
    final String password =
        conf.getTrimmed(DFSConfigKeys.DFS_ZONE_GENERATE_REPLICATION_RULE_KAFKA_PASSWORD);
    final String bootstrapServers =
        conf.getTrimmed(DFSConfigKeys.DFS_ZONE_GENERATE_REPLICATION_RULE_KAFKA_BOOTSTRAP_SERVERS);
    final String topic =
        conf.getTrimmed(DFSConfigKeys.DFS_ZONE_GENERATE_REPLICATION_RULE_KAFKA_TOPIC);
    final String groupId =
        conf.getTrimmed(DFSConfigKeys.DFS_ZONE_GENERATE_REPLICATION_RULE_KAFKA_GROUP_ID);
    final int requestTimeOut =
        conf.getInt(DFSConfigKeys.DFS_ZONE_GENERATE_REPLICATION_RULE_KAFKA_REQUEST_TIMEOUT_MS,
            DFSConfigKeys.DFS_ZONE_GENERATE_REPLICATION_RULE_KAFKA_REQUEST_TIMEOUT_DEFAULT);
    final int maxPollRecords =
        conf.getInt(DFSConfigKeys.DFS_ZONE_GENERATE_REPLICATION_RULE_KAFKA_MAX_POLL_RECORDS_KEY,
            DFSConfigKeys.DFS_ZONE_GENERATE_REPLICATION_RULE_KAFKA_MAX_POLL_RECORDS_DEFAULT);
    final int maxPollIntervalMs =
        conf.getInt(DFSConfigKeys.DFS_ZONE_GENERATE_REPLICATION_RULE_KAFKA_MAX_POLL_INTERVAL_MS,
            DFSConfigKeys.DFS_ZONE_GENERATE_REPLICATION_RULE_KAFKA_MAX_POLL_INTERVAL_MS_DEFAULT);
    final int sessionTimeOutMs =
        conf.getInt(DFSConfigKeys.DFS_ZONE_GENERATE_REPLICATION_RULE_KAFKA_SESSION_TIMEOUT_MS,
            DFSConfigKeys.DFS_ZONE_GENERATE_REPLICATION_RULE_KAFKA_SESSION_TIMEOUT_MS_DEFAULT);

    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        bootstrapServers);
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeOut);
    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
    properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);
    properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeOutMs);

    properties.setProperty("security.protocol", "SASL_PLAINTEXT");
    properties.setProperty("sasl.mechanism", "PLAIN");
    properties.setProperty("sasl.jaas.config",
        "org.apache.kafka.common.security.plain.PlainLoginModule " +
            "required username=\""+username+"\" password=\""+password+"\";");
    this.consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(Collections.singletonList(topic));
    Set<TopicPartition> assignment = Sets.newHashSet();
    while (assignment.size() == 0) {
      consumer.poll(100L);
      assignment = consumer.assignment();
    }
    consumer.seekToEnd(assignment);
  }

  /**
   * handle kafka message:
   * {"clientDC":"/Telin-1","dnDC":"/Telin-4","size":100000000,
   * "logTimestamp":"2022-02-18 08:32:22,"ns":"tl5","num":3,
   * "path":"/user/data_datahub/logingestion1"}
   */
  private void runReplicationRuleGenerate() {
    while (!Thread.interrupted()) {
      try {
        ConsumerRecords<String, String> records = consumer.poll(pollTimeOut);
        if (!records.isEmpty()) {
          for (ConsumerRecord<String, String> record : records) {
            // Limit the number of concurrent processing threads for kafka messages,
            // to avoid program OOM.
            rateLimiter.acquire();
            processRecord(record.value());
          }
        }
      } catch (InterruptedException ie) {
        LOG.error("ReplicationRuleGenerateKafkaTrigger thread interrupted will exit.", ie);
        Thread.currentThread().interrupt();
      } catch (Exception ex) {
        LOG.error("ReplicationRuleGenerateKafkaTrigger thread unexpected exception " +
            "will continue to run.", ex);
        if (consumer != null) {
          try {
            consumer.close();
          } catch (Exception exception) {
            LOG.error("Ignore kafka consumer close exception", exception);
          }
        }
        initKafka();
      }
    }
  }

  private void processRecord(String record) {
    executor.execute(new AddRule(record));
  }

  private void setReplicationRuleParam(Configuration conf) {
    ruleGenerateKey = conf.getTrimmed(DFSConfigKeys.DFS_ZONE_GENERATE_REPLICATION_RULE_KEY,
        DFSConfigKeys.DFS_ZONE_GENERATE_REPLICATION_RULE_DEFAULT);
    if (ruleGenerateKey.split(FIELD_SEPARATOR).length != 2) {
      ruleGenerateKey = DFSConfigKeys.DFS_ZONE_GENERATE_REPLICATION_RULE_DEFAULT;
    }

    pathSizeLimit = conf.getLong(
        DFSConfigKeys.DFS_ZONE_GENERATE_REPLICATION_RULE_PATH_SIZE_LIMIT_KEY,
        DFSConfigKeys.DFS_ZONE_GENERATE_REPLICATION_RULE_PATH_SIZE_LIMIT_DEFAULT);

    minCrossReadSize = conf.getLong(
        DFSConfigKeys.DFS_ZONE_GENERATE_REPLICATION_RULE_MIN_CROSS_RAEAD_SIZE_KEY,
        DFSConfigKeys.DFS_ZONE_GENERATE_REPLICATION_RULE_MIN_CROSS_RAEAD_SIZE_DEFAULT);
    pollTimeOut = conf.getInt(
        DFSConfigKeys.DFS_ZONE_GENERATE_REPLICATION_RULE_KAFKA_POLL_TIMEOUT_MS,
        DFSConfigKeys.DFS_ZONE_GENERATE_REPLICATION_RULE_KAFKA_POLL_TIMEOUT_DEFAULT);

    capacityLimit = conf.getLong(
        DFSConfigKeys.DFS_ZONE_GENERATE_REPLICATION_RULE_FILTER_PATHS_CAPACITY_LIMIT_KEY,
        DFSConfigKeys.DFS_ZONE_GENERATE_REPLICATION_RULE_FILTER_PATHS_CAPACITY_LIMIT_DEFAULT);

    supportMigrateReplica = conf.getBoolean(
        DFSConfigKeys.DFS_ZONE_SUPPORT_MIGRATE_REPLICA_ENABLED_KEY,
        DFSConfigKeys.DFS_ZONE_SUPPORT_MIGRATE_REPLICA_ENABLED_KEY_DEFAULT);

    validDataCenters = new HashSet<>(
        conf.getTrimmedStringCollection(DFSConfigKeys.DFS_ZONEMOVER_VALID_DATACENTERS_KEY));

    maxRateLimit = conf.getInt(DFSConfigKeys.DFS_ZONE_GENERATE_REPLICATION_RULE_MAX_RATE_LIMIET_KEY,
        DFSConfigKeys.DFS_ZONE_GENERATE_REPLICATION_RULE_MAX_RATE_LIMIET_DEFAULT);

    loadBlacklist(conf);

    LOG.info("Init ReplicationRuleParam with ruleGenerateKey = {}, pathSizeLimit = {}, "
            + "minCrossReadSize = {}, pollTimeOut = {}, capacityLimit = {} , "
            + "supportMigrateReplica = {}, validDataCenters = {}, maxRateLimit = {} ",
        ruleGenerateKey, pathSizeLimit, minCrossReadSize, pollTimeOut,
        capacityLimit, supportMigrateReplica, validDataCenters, maxRateLimit);
  }

  private void loadBlacklist(Configuration conf) {
    String blacklistFile = conf.get(
        DFSConfigKeys.DFS_ZONE_GENERATE_REPLICATION_RULE_BLACKLIST_FILE_KEY);
    if (blacklistFile == null || blacklistFile.isEmpty()) {
      LOG.warn( "{} not configured.",
          DFSConfigKeys.DFS_ZONE_GENERATE_REPLICATION_RULE_BLACKLIST_FILE_KEY);
      return;
    }
    File file = new File(blacklistFile);
    try {
      loadBlacklistInternal(file);
    } catch (IOException e) {
      LOG.error("Error load blacklist paths.", e);
    }
  }

  private void loadBlacklistInternal(File file) throws IOException {
    if (!file.exists()) {
      LOG.warn("{} does not exist.", file);
      return;
    }
    // new set.
    Set<String> newLinesSet = new HashSet<>();
    LOG.info("Loading new lines from file: {}.", file);
    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(new FileInputStream(file), Charsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Loading new lines from file: handle {}.", line);
        }

        // skip empty line and comment
        line = line.trim();
        if (line.isEmpty() || line.startsWith("#")) {
          continue;
        }

        if (!line.endsWith("/")) {
          line += "/";
        }

        newLinesSet.add(line);
      }
    }

    LOG.info("Loaded {} from {}.", newLinesSet.size(), file);
    if (newLinesSet.size() == 0) {
      LOG.warn("Lines set is empty.");
    }
    this.blacklistPaths = newLinesSet;
  }

  private boolean skipPath(String path) {
    return blacklistPaths.stream().anyMatch(path::startsWith);
  }

  private class Monitor implements Runnable {
    @Override
    public void run() {
      runReplicationRuleGenerate();
    }
  }

  private class AddRule implements Runnable {
    private String record;

    public AddRule(String record) {
      this.record = record;
    }

    @Override
    public void run() {
      try {
        JSONObject jsonObject = new JSONObject(record);
        String clientDC = jsonObject.getString("clientDC");
        String dnDC = jsonObject.getString("dnDC");
        String ns = jsonObject.getString("ns");
        String path = jsonObject.getString("path");
        long crossReadSize = jsonObject.getLong("size");
        if (skipPath(path)) {
          LOG.info("{} will be skipped.", path);
          return;
        }
        if (!validDataCenters.contains(clientDC) || !validDataCenters.contains(dnDC)) {
          LOG.warn("Can not add invalid replication rule: {}.", record);
          return;
        }
        if (filterPaths.contains(path)) {
          LOG.warn("Can not add replication rule: {} and filterPaths contain {} skip.", record,
              path);
          return;
        }
        long start = now();
        ContentSummary contentSummary = fs.getContentSummary(new Path(path));
        long pathSize = contentSummary.getLength();

        if (pathSize <= pathSizeLimit && crossReadSize >= minCrossReadSize) {
          if (supportMigrateReplica) {
            // If `supportMigrateReplica` is enabled and `validDataCenters` as  [/STT,/YTL,/AT].
            if (validDataCenters.size() < 3) {
              LOG.warn("Can not add replication rule: {} due validDataCenters {} is invalid.",
                  record, validDataCenters);
              return;
            }

            // The file replica rules in the migration process are dynamically generated according
            // to the number of replicas. Here, for the compatibility of code implementation,
            // replicationRule is set to the default value.
            // Refer to SPDI-137059.
            String replicationRule = "/migrateDC:1";
            LOG.info("{} {} add replication rule: {} with clientDC: {} and dnDC:{} " +
                    "start for migration.", ns, path, replicationRule, clientDC, dnDC);
            ResultCode resultCode =
                replicationRuleManager.createReplicaRulesByMigration(ns, path,
                    replicationRule, true);
            LOG.info("{} {} add replication rule: {} with clientDC: {} and dnDC:{}," +
                    " code: {} and cost {} ms.", ns, path, replicationRule, clientDC, dnDC,
                resultCode.getMsg(), now() - start);
            return;
          } else {
            String[] rules = ruleGenerateKey.split(FIELD_SEPARATOR);
            if (rules.length == 2) {
              String replicationRule =
                  new StringBuilder().append(clientDC).append(FIELD_SEPARATOR).append(rules[0])
                      .append(SECTION_SEPARATOR).append(dnDC).append(FIELD_SEPARATOR)
                      .append(rules[1]).toString();
              LOG.info("{} {} add replication rule: {} start.", ns, path, replicationRule);
              ResultCode resultCode =
                  replicationRuleManager.createUpdateMap(ns, path, replicationRule, true);
              LOG.info("{} {} add replication rule: {} {} and cost {} ms.", ns, path,
                  replicationRule, resultCode.getMsg(), now() - start);
              return;
            }
          }
        }

        if (pathSize > pathSizeLimit) {
          if (filterPaths.size() < capacityLimit) {
            filterPaths.add(path);
          } else {
            LOG.warn("Can not add {} to filterPaths and " + "capacity = {}, limit = {}.", path,
                filterPaths.size(), capacityLimit);
          }
        }
        LOG.warn("Can not add replication rule: {} and cost {} ms.", record, now() - start);
      } catch (Exception e) {
        LOG.error("Failed to add replication rule: {}", record, e);
      } finally {
        rateLimiter.release();
      }
    }
  }
}