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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

import static org.apache.hadoop.util.Time.now;

/**
 * The class is supports automatically generate replication rules
 * from kakfa record when heavy sudden traffic happens.
 */
public class ReplicationRuleGenerateKafkaTrigger {

  private static final Logger LOG = LoggerFactory.getLogger(
      ReplicationRuleGenerateKafkaTrigger.class);

  private final Consumer<String, String> consumer;
  private final DistributedFileSystem fs;
  private String ruleGenerateKey;
  private long pathSizeLimit;
  private long minCrossReadSize;
  private int pollTimeOut;
  // "," is the separator of pattern "/dc1:replica1,/dc2:replica2"
  private final static String SECTION_SEPARATOR = ",";
  private final static String FIELD_SEPARATOR = ":";
  private final ReplicationRuleManager replicationRuleManager;
  private final Thread monitorServer;
  private final ExecutorService executor;

  public ReplicationRuleGenerateKafkaTrigger(Configuration conf) throws IOException {
    final String username =
        conf.getTrimmed(DFSConfigKeys.DFS_ZONE_GENERTE_REPLICATION_RULE_KAFKA_USERNAME);
    final String password =
        conf.getTrimmed(DFSConfigKeys.DFS_ZONE_GENERTE_REPLICATION_RULE_KAFKA_PASSWORD);
    final String bootstrapServers =
        conf.getTrimmed(DFSConfigKeys.DFS_ZONE_GENERTE_REPLICATION_RULE_KAFKA_BOOTSTRAP_SERVERS);
    final String topic =
        conf.getTrimmed(DFSConfigKeys.DFS_ZONE_GENERTE_REPLICATION_RULE_KAFKA_TOPIC);
    final String groupId =
        conf.getTrimmed(DFSConfigKeys.DFS_ZONE_GENERTE_REPLICATION_RULE_KAFKA_GROUP_ID);
    final int requestTimeOut =
        conf.getInt(DFSConfigKeys.DFS_ZONE_GENERTE_REPLICATION_RULE_KAFKA_REQUEST_TIMEOUT_MS,
            DFSConfigKeys.DFS_ZONE_GENERTE_REPLICATION_RULE_KAFKA_REQUEST_TIMEOUT_DEFAULT);

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

    properties.setProperty("security.protocol", "SASL_PLAINTEXT");
    properties.setProperty("sasl.mechanism", "PLAIN");
    properties.setProperty("sasl.jaas.config",
        "org.apache.kafka.common.security.plain.PlainLoginModule " +
            "required username=\""+username+"\" password=\""+password+"\";");

    this.fs = (DistributedFileSystem) FileSystem.get(conf);
    this.replicationRuleManager = new ReplicationRuleManager(conf);
    this.consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(Collections.singletonList(topic));

    setReplicationRuleParam(conf);
    this.monitorServer = new Thread(new Monitor(), "replicationRuleGenerateKafkaTrigger");
    this.monitorServer.start();
    executor = HadoopExecutors.newCachedThreadPool(
        new ThreadFactoryBuilder()
            .setNameFormat("addReplicationRule #%d")
            .build());
  }

  public void shutdown() {
    if (consumer != null) {
      consumer.close();
    }
    if (monitorServer != null) {
      monitorServer.interrupt();
    }
    if (executor != null) {
      executor.shutdown();
    }
  }

  /**
   * handle kafka message:
   * {"clientDC":"/Telin-1","dnDC":"/Telin-4","size":100000000,
   * "logTimestamp":"2022-02-18 08:32:22,"ns":"tl5","num":3,
   * "path":"/user/data_datahub/logingestion1"}
   */
  private void runReplicationRuleGenerate() {
    try {
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(pollTimeOut);
        for (ConsumerRecord<String, String> record : records) {
          processRecord(record.value());
        }
      }
    } catch (Exception e) {
      LOG.error("Unexpected exception", e);
    } finally {
      shutdown();
    }
  }

  private void processRecord(String record) {
    executor.execute(new AddRule(record));
  }

  private void setReplicationRuleParam(Configuration conf) {
    ruleGenerateKey = conf.getTrimmed(DFSConfigKeys.DFS_ZONE_GENERTE_REPLICATION_RULE_KEY,
        DFSConfigKeys.DFS_ZONE_GENERTE_REPLICATION_RULE_DEFAULT);
    if (ruleGenerateKey.split(FIELD_SEPARATOR).length != 2) {
      ruleGenerateKey = DFSConfigKeys.DFS_ZONE_GENERTE_REPLICATION_RULE_DEFAULT;
    }

    pathSizeLimit = conf.getLong(DFSConfigKeys.DFS_ZONE_GENERTE_REPLICATION_RULE_PATH_SIZE_LIMIT_KEY,
        DFSConfigKeys.DFS_ZONE_GENERTE_REPLICATION_RULE_PATH_SIZE_LIMIT_DEFAULT);

    minCrossReadSize = conf.getLong(
        DFSConfigKeys.DFS_ZONE_GENERTE_REPLICATION_RULE_MIN_CROSS_RAEAD_SIZE_KEY,
        DFSConfigKeys.DFS_ZONE_GENERTE_REPLICATION_RULE_MIN_CROSS_RAEAD_SIZE_DEFAULT);
    pollTimeOut = conf.getInt(
        DFSConfigKeys.DFS_ZONE_GENERTE_REPLICATION_RULE_KAFKA_POLL_TIMEOUT_MS,
        DFSConfigKeys.DFS_ZONE_GENERTE_REPLICATION_RULE_KAFKA_POLL_TIMEOUT_DEFAULT);
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
        long start = now();
        JSONObject jsonObject = new JSONObject(record);
        String clientDC = jsonObject.getString("clientDC");
        String dnDC = jsonObject.getString("dnDC");
        String ns = jsonObject.getString("ns");
        String path = jsonObject.getString("path");
        long crossReadSize = jsonObject.getLong("size");
        ContentSummary contentSummary = fs.getContentSummary(new Path(path));
        long pathSize = contentSummary.getLength();
        String[] rules = ruleGenerateKey.split(FIELD_SEPARATOR);
        if (rules.length == 2 && pathSize <= pathSizeLimit &&
            crossReadSize >= minCrossReadSize) {
          String replicationRule = new StringBuilder().
              append(clientDC).append(FIELD_SEPARATOR).append(rules[0]).
              append(SECTION_SEPARATOR).
              append(dnDC).append(FIELD_SEPARATOR).append(rules[1]).toString();
          LOG.info("{} {} add replication rule: {} start.", ns, path, replicationRule);
          ResultCode resultCode =
              replicationRuleManager.createUpdateMap(ns, path, replicationRule, true);
          LOG.info("{} {} add replication rule: {} {} and cost {} ms.", ns, path, replicationRule,
              resultCode.getMsg(), now() - start);
        } else {
          LOG.warn("Can not add replication rule: {} and cost {} ms.", record, now() - start);
        }
      } catch (Exception e) {
        LOG.error("Failed to add replication rule: {}", record, e);
      }
    }
  }
}