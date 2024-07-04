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

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.zoneservice.metrics.ZoneMoverMetrics;
import org.apache.hadoop.hdfs.server.zoneservice.store.KafkaTopicRecord;
import org.apache.hadoop.hdfs.server.zoneservice.store.Query;
import org.apache.hadoop.hdfs.server.zoneservice.store.StoreDriver;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;

public class ZoneMoverKafkaTrigger extends ZoneMoverTrigger {
  private static final Logger LOG = LoggerFactory.getLogger(ZoneMover.class);

  protected final String nameSpace;
  protected String groupId;
  protected List<Path> monitorPaths;
  private long lastZkUpdateTime;
  private final long zkUpdateIntervalMs;
  protected ExecutorService executorService;
  private StoreDriver driver;

  protected final BlockingQueue<Pair<ConsumerRecord<String, String>, String>> recordQueue;

  protected final Collection<String> skipRenameKeywords;
  protected final Collection<String> skipCompleteKeywords;

  static final List<String> CARE_LOG_SYMBOL = new ArrayList() {{
    add("allowed=");
    add("src=");
    add("dst=");
  }};

  public ZoneMoverKafkaTrigger(Configuration conf,
      List<Path> paths, URI namenode) {
    this(conf, paths, namenode, false);
  }

  //Init HDFS audit log kafka consumer
  public ZoneMoverKafkaTrigger(Configuration conf,
      List<Path> paths, URI namenode, boolean useZK) {
    nameSpace = namenode.getAuthority();
    final String username =
        conf.get(DFSConfigKeys.DFS_ZONEMOVER_KAFKA_USERNAME);
    final String password =
        conf.get(DFSConfigKeys.DFS_ZONEMOVER_KAFKA_PASSWORD);
    final String bootstrapServers =
        conf.get(DFSConfigKeys.DFS_ZONEMOVER_KAFKA_BOOTSTRAP_SERVERS);
    final String nsTopic =
        conf.get(DFSConfigKeys.DFS_ZONEMOVER_KAFKA_TOPIC_WITH_NAMESPACE_PREFIX + nameSpace);
    final String topic = nsTopic != null ? nsTopic :
        conf.get(DFSConfigKeys.DFS_ZONEMOVER_KAFKA_TOPIC);
    groupId =
        conf.get(DFSConfigKeys.DFS_ZONEMOVER_KAFKA_GROUP_ID);

    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        bootstrapServers);
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    groupId = groupId + "_" + nameSpace;
    properties.put("group.id", groupId);

    properties.setProperty("security.protocol", "SASL_PLAINTEXT");
    properties.setProperty("sasl.mechanism", "PLAIN");
    properties.setProperty("sasl.jaas.config",
        "org.apache.kafka.common.security.plain.PlainLoginModule " +
            "required username=\""+username+"\" password=\""+password+"\";");
    Consumer<String, String> consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(Collections.singletonList(topic));
    // Get partition information.
    consumer.poll(0);
    Set<TopicPartition> partitions = consumer.assignment();
    consumer.close();
    assert partitions != null && partitions.size() > 0 :
        "The partition of Kafka topic: " + topic + " cannot be empty.";

    final int queueSize =
        conf.getInt(DFSConfigKeys.DFS_ZONEMOVER_TRIGGER_QUEUE_SIZE_KEY,
            DFSConfigKeys.DFS_ZONEMOVER_TRIGGER_QUEUE_SIZE_DEFAULT);

    if (useZK) {
      Class<? extends StoreDriver> driverClass = conf.getClass(
          DFSConfigKeys.DFS_ZONESERVICE_STORE_DRIVER_CLASS,
          DFSConfigKeys.DFS_ZONESERVICE_STORE_DRIVER_CLASS_DEFAULT,
          StoreDriver.class);
      driver = ReflectionUtils.newInstance(driverClass, conf);
      driver.init(conf, "ZoneMoverKafkaTrigger");
    }
    zkUpdateIntervalMs = conf.getLong(
        DFSConfigKeys.DFS_ZONEMOVER_TRIGGER_ZK_UPDATE_OFFSET_INTERVAL_KEY,
        DFSConfigKeys.DFS_ZONEMOVER_TRIGGER_ZK_UPDATE_OFFSET_INTERVAL_DEFAULT);
    lastZkUpdateTime = Time.monotonicNow();

    recordQueue = new LinkedBlockingQueue<>(queueSize);
    skipCompleteKeywords =
        conf.getStringCollection(DFSConfigKeys.DFS_ZONEMOVER_TRIGGER_SKIP_COMPLETE_KEYWORDS_KEY);
    skipRenameKeywords =
        conf.getStringCollection(DFSConfigKeys.DFS_ZONEMOVER_TRIGGER_SKIP_RENAME_KEYWORDS_KEY);

    LOG.info("Starting {} KafkaConsumerPool threads for namespace '{}'.", partitions.size(),
        nameSpace);
    executorService = Executors.newFixedThreadPool(partitions.size(),
        new ThreadFactory() {
          @Override
          public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setName("KafkaConsumerPool-Thread-" + nameSpace + "-" + thread.getId());
            return thread;
          }
        });

    // Start one thread per partition.
    for (TopicPartition partition : partitions) {
      executorService.submit(new MonitorTask(properties, partition, driver, nameSpace, groupId));
    }

    monitorPaths = paths;
    LOG.info("ZoneMover trigger for {} has been started!", nameSpace);
    LOG.info("monitorPaths:{}, {}:{}, {}:{}", monitorPaths,
        DFSConfigKeys.DFS_ZONEMOVER_TRIGGER_SKIP_COMPLETE_KEYWORDS_KEY,skipCompleteKeywords,
        DFSConfigKeys.DFS_ZONEMOVER_TRIGGER_SKIP_RENAME_KEYWORDS_KEY, skipRenameKeywords);
  }

  @Override
  public boolean hasNext() {
    return true;
  }

  @Override
  public String getNext() throws InterruptedException {
    return recordQueue.take().getRight();
  }

  @Override
  public Pair<ConsumerRecord<String, String>, String> getNextRecord()
      throws InterruptedException {
    return recordQueue.take();
  }

  @Override
  public String getGroupId() {
    return groupId;
  }

  @Override
  public void saveOffsetToZookeeper(ConsumerRecord<String, String> record, String ns,
      String groupId, ZoneMoverMetrics zoneMoverMetrics) {
    long now = Time.monotonicNow();
    if (driver == null || now - lastZkUpdateTime < zkUpdateIntervalMs) {
      return;
    }
    KafkaTopicRecord kafkaTopicRecord = new KafkaTopicRecord(ns, record.topic(), groupId,
        record.partition(), record.offset());
    try {
      KafkaTopicRecord existingKafkaTopicRecord = driver.get(new Query<>(kafkaTopicRecord),
          KafkaTopicRecord.class);
      if (existingKafkaTopicRecord != null) {
        if (kafkaTopicRecord.getOffset() < existingKafkaTopicRecord.getOffset()) {
          return;
        }
      }
      driver.put(kafkaTopicRecord, true, false);
      lastZkUpdateTime = now;
      LOG.info("SaveOffsetToZookeeper with {} taken: {} ms", kafkaTopicRecord,
          Time.monotonicNow() - now);
      zoneMoverMetrics.addKafkaOffsetZk(Time.monotonicNow() - now);
    } catch (IOException e) {
      LOG.error("Failed to saveOffsetToZookeeper {}.", kafkaTopicRecord, e);
    }
  }

  /**
   * Process HDFS audit log to a json object
   * For example:
   * The HDFS audit log is like:
   *    XXX INFO XXX: allowed=true ugi=A ip=xxx cmd=xxx src=/A dst=/B
   * After processMessage, we will drop the log head and
   * we can get a jsonObject from which we can get the option
   * we want in audit log like jsonObject.get("ugi")->"A"
   */
  protected static JSONObject message2json(String rawMessage) {
    JSONObject jsonObject = new JSONObject();
    try {
      List<String> listRawString = extractCompletePath(rawMessage);
      List<String> listString = new ArrayList<>();
      int index = 0;
      String curSymbol = CARE_LOG_SYMBOL.get(index);
      for (String item : listRawString) {
        if (item.startsWith(curSymbol)) {
          listString.add(item.replaceFirst("=", "\":\""));
          index++;
          if (index >= CARE_LOG_SYMBOL.size()) {
            break;
          }
          curSymbol = CARE_LOG_SYMBOL.get(index);
        }
      }
      String message = "\"" + StringUtils
          .join("\",\"",listString) + "\"";
      jsonObject = new JSONObject('{' + message + '}');
      return jsonObject;
    } catch (JSONException e) {
      LOG.warn("[JSON] Audit log format is irregular: " + rawMessage);
    } catch (ArrayIndexOutOfBoundsException e) {
      LOG.warn("[INDEX] Audit log format is irregular: " + rawMessage);
    }
    return jsonObject;
  }

  /**
   * Extract complete path from audit log, no matter what kind of special character path contains
   */
  private static List<String> extractCompletePath(String rawMessage)
      throws ArrayIndexOutOfBoundsException {
    List<String> result;
    String[] s1 = rawMessage.split("src=", 2);
    String[] s2 = s1[1].split("dst=", 2);
    String[] s3 = s2[1].split("perm=", 2);
    result = new ArrayList<>(Arrays.asList(s1[0].split("[ \t]")));
    result.add("src=" + s2[0].trim());
    result.add("dst=" + s3[0].trim());
    List<String> tmpList = Arrays.asList(("perm=" + s3[1].trim()).split("[ \t]"));
    result.addAll(tmpList);
    // Ignore "callContext" info in audit log
    result.remove(result.size() -1);
    return result;
  }

  /**
   * Check if the path is under the monitor paths
   * @param curPath path need to be checked
   * @return TRUE means current path is under monitor paths, need to process it
   *         FALSE means current path isn't under monitor paths,
   *         no need to process
   */
  protected boolean checkPaths(String curPath) {
    for (Path path : monitorPaths) {
      if (curPath.startsWith(path.toString())) {
        return true;
      }
    }
    return false;
  }

  /**
   * return true if path contains any keywords
   */
  boolean containKeyWords(String path, Collection<String> keySet) {
    for (Iterator<String> it = keySet.iterator(); it.hasNext();) {
      if (path.indexOf(it.next()) > -1) {
        return true;
      }
    }
    LOG.debug("In containKeyWords: path: {}, keySet: {}", path, keySet);
    return false;
  }

  @Override
  public void updatePaths(List<Path> paths) {
    monitorPaths = paths;
  }

  @Override
  public void shutdown() {
    executorService.shutdown();
  }

  class MonitorTask implements Runnable {
    private Consumer<String, String> consumer;
    private TopicPartition topicPartition;
    private StoreDriver driver;
    private String ns;
    private String groupId;
    public MonitorTask(Properties properties, TopicPartition topicPartition,
        StoreDriver driver, String ns, String groupId) {
      properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
      this.consumer = new KafkaConsumer<>(properties);
      this.topicPartition = topicPartition;
      this.driver = driver;
      this.ns = ns;
      this.groupId = groupId;
      consumer.assign(Collections.singletonList(topicPartition));
    }
    @Override
    public void run() {
      monitorPaths();
    }

    public void monitorPaths() {
      setInitialOffsetFromZookeeper();
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(100);
        try {
          for (ConsumerRecord<String, String> record: records) {
            String rawMessage = record.value();
            JSONObject jsonObject = new JSONObject(rawMessage);
            //Filter the record doesn't belong to this namespace
            if (!jsonObject.get("ns").equals(nameSpace)) {
              continue;
            }
            String message = jsonObject.get("message").toString();

            String path = processMessage(message);
            if (path != null) {
              recordQueue.put(new ImmutablePair<>(record, path));
            }
          }
        } catch (JSONException e) {
          LOG.warn("processMessage encountered exception.", e);
        } catch (InterruptedException e) {
          LOG.info("ZoneMover trigger thread is interrupted!");
          break;
        }
      }
    }

    /**
     * Choose new files from HDFS audit log
     */
    private String processMessage(String message) throws JSONException, InterruptedException {
      if (message.contains("cmd=complete")) {
        JSONObject jsonMessage = message2json(message);
        if (jsonMessage.get("allowed").equals("true")) {
          String src = jsonMessage.get("src").toString();
          if (!containKeyWords(src, skipCompleteKeywords) && checkPaths(src)) {
            LOG.debug("New create file: {}", src);
            return src;
          }
        }
      } else if (message.contains("cmd=rename")) {
        JSONObject jsonMessage = message2json(message);
        if (jsonMessage.get("allowed").equals("true")) {
          String src = jsonMessage.get("src").toString();
          String dst = jsonMessage.get("dst").toString();
          if (!containKeyWords(dst, skipRenameKeywords) && checkPaths(dst)) {
            LOG.debug("New rename file, source: {}, destination: {}", src, dst);
            return dst;
          }
        }
      }
      return null;
    }

    private void setInitialOffsetFromZookeeper() {
      if (driver == null) {
        return;
      }
      KafkaTopicRecord kafkaTopicRecord = new KafkaTopicRecord(ns, topicPartition.topic(),
          groupId, topicPartition.partition(), 0);
      try {
        KafkaTopicRecord existingKafkaTopicRecord = driver.get(new Query<>(kafkaTopicRecord),
            KafkaTopicRecord.class);
        if (existingKafkaTopicRecord != null) {
          long newOffset = existingKafkaTopicRecord.getOffset() + 1;
          consumer.seek(topicPartition, newOffset);
          LOG.info("Init set new offset: {} by existingKafkaTopicRecord: {}.", newOffset,
              existingKafkaTopicRecord);
        }
      } catch (IOException e) {
        LOG.error("Failed to setInitialOffsetFromZookeeper {}.", kafkaTopicRecord, e);
      }
    }
  }
}