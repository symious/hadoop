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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.util.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;

public class ZoneAutoBalancerTrigger {
  private static final Logger LOG = LoggerFactory.getLogger(ZoneMover.class);

  private final Consumer<String, String> consumer;
  private final String topic;

  private final ReplicationRuleUtil replicationRuleUtil;

  private final int queueSize;

  private long timestamp;
  private final long setOffsetInterval;
  private final List<String> nsWhiteList;

  public ZoneAutoBalancerTrigger(Configuration conf) throws IOException {
    replicationRuleUtil = new ReplicationRuleUtil(FileSystem.get(conf));

    queueSize = conf.getInt(
        DFSConfigKeys.DFS_ZONESERVICE_AUTO_BALANCER_QUEUE_SIZE_KEY,
        DFSConfigKeys.DFS_ZONESERVICE_AUTO_BALANCER_QUEUE_SIZE_DEFAULT);
    setOffsetInterval = conf.getLong(
        DFSConfigKeys.DFS_ZONESERVICE_AUTO_BALANCER_SET_OFFSET_INTERVAL_KEY,
        DFSConfigKeys.DFS_ZONESERVICE_AUTO_BALANCER_SET_OFFSET_INTERVAL_DEFAULT);
    String whiteListString = conf.get(
        DFSConfigKeys.DFS_ZONESERVICE_AUTO_BALANCER_NS_WHITE_LIST_KEY,
        DFSConfigKeys.DFS_ZONESERVICE_AUTO_BALANCER_NS_WHITE_LIST_DEFAULT);
    if (whiteListString.trim().isEmpty()) {
      nsWhiteList = new ArrayList<>();
      LOG.info("No namespace white list is found, all the namespaces will be processed!");
    } else {
      nsWhiteList = Arrays.asList(whiteListString.trim().split(","));
      LOG.info("Namespace white list is {}", nsWhiteList);
    }
    timestamp = System.currentTimeMillis();

    final String username =
        conf.get(DFSConfigKeys.DFS_ZONEMOVER_KAFKA_USERNAME);
    final String password =
        conf.get(DFSConfigKeys.DFS_ZONEMOVER_KAFKA_PASSWORD);
    final String bootstrapServers =
        conf.get(DFSConfigKeys.DFS_ZONEMOVER_KAFKA_BOOTSTRAP_SERVERS);
    topic =
        conf.get(DFSConfigKeys.DFS_ZONEMOVER_KAFKA_TOPIC);
    final String groupId =
        conf.get(DFSConfigKeys.DFS_ZONEMOVER_KAFKA_GROUP_ID);

    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        bootstrapServers);
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    properties.put("group.id", groupId + "_zab");

    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    properties.setProperty("security.protocol", "SASL_PLAINTEXT");
    properties.setProperty("sasl.mechanism", "PLAIN");
    properties.setProperty("sasl.jaas.config",
        "org.apache.kafka.common.security.plain.PlainLoginModule " +
            "required username=\""+username+"\" password=\""+password+"\";");

    consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(Collections.singletonList(topic));

    LOG.info("ZoneService Auto Balancer trigger has been started!");
  }

  public void monitorPaths(Map<String, ArrayBlockingQueue<MoverPathInfo>> pathQueue,
      Map<Integer, List<Long>> offsetMap) {
    Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = new HashMap<>();
    while (true) {
      //Set offset for every partition
      if ((System.currentTimeMillis() - timestamp) > setOffsetInterval) {
        for (int par: offsetMap.keySet()) {
          TopicPartition topicPartition = new TopicPartition(topic, par);
          try {
            OffsetAndMetadata offset = new OffsetAndMetadata(offsetMap.get(par).get(0));
            offsetAndMetadataMap.put(topicPartition, offset);
          } catch (IndexOutOfBoundsException e) {
            offsetMap.remove(par);
            offsetAndMetadataMap.remove(topicPartition);
          }
        }
        if (offsetAndMetadataMap.isEmpty()) {
          LOG.info("There is no record about partition offset, will set it to the offset " +
              "which is consuming");
          consumer.commitSync();
          timestamp = System.currentTimeMillis();
        } else {
          consumer.commitSync(offsetAndMetadataMap);
          timestamp = System.currentTimeMillis();
        }
      }

      ConsumerRecords<String, String> records = consumer.poll(100);
      try {
        for (ConsumerRecord<String, String> record : records) {
          String rawMessage = record.value();
          JSONObject jsonObject = new JSONObject(rawMessage);
          String ns = jsonObject.get("ns").toString();
          // Filter namespace according to white list
          if (!nsWhiteList.isEmpty() && !nsWhiteList.contains(ns)) {
            LOG.debug("Message is skipped by namespace white list.\n {}", rawMessage);
            continue;
          }

          String message = jsonObject.get("message").toString();
          JSONObject jsonAuditLog = processMessage(message);

          if (jsonAuditLog != null) {
            String validPath = jsonAuditLog.get("src").toString();
            long dst;
            dst = Long.parseLong(jsonAuditLog.get("dst").toString());

            int par = record.partition();
            long offset = record.offset();
            if (pathQueue.containsKey(ns)) {
              pathQueue.get(ns).put(new MoverPathInfo(par, offset, validPath, dst));
            } else {
              pathQueue.put(ns, new ArrayBlockingQueue<MoverPathInfo>(queueSize));
              pathQueue.get(ns).put(new MoverPathInfo(par, offset, validPath, dst));
            }
            // The message should be processed in order
            if (offsetMap.containsKey(par)) {
              offsetMap.get(par).add(offset);
            } else {
              offsetMap.put(par, new CopyOnWriteArrayList<>());
              offsetMap.get(par).add(offset);
            }
          }
        }
      } catch (JSONException e) {
        LOG.error("Error when convert audit log to json: ", e);
      } catch (Throwable e) {
        LOG.info("ZoneService auto balance trigger thread is interrupted!");
        break;
      }
    }
  }

  /**
   * Choose new files from HDFS audit log
   */
  private JSONObject processMessage(String message) throws JSONException {
    if (message.contains("cmd=complete")) {
      JSONObject jsonMessage = message2json(message);
      if (jsonMessage.get("allowed").equals("true")) {
        LOG.debug("New create file: " +
            jsonMessage.get("src").toString());
        try {
          replicationRuleUtil.autoSetReplicaRule(jsonMessage.get("src").toString(),
              jsonMessage.getLong("dst") );
        } catch (IOException e) {
          LOG.warn("Set replica rule fail! Will try it one more time.");
        }
        return jsonMessage;
      }
    } else if (message.contains("cmd=setReplication")) {
      JSONObject jsonMessage = message2json(message);
      if (jsonMessage.get("allowed").equals("true")) {
        LOG.debug("Set replication for file: " +
            jsonMessage.get("src").toString());
        try {
          replicationRuleUtil.autoSetReplicaRule(jsonMessage.get("src").toString(),
              jsonMessage.getInt("dst") );
        } catch (IOException e) {
          LOG.warn("Set replica rule fail! Will try it one more time.");
        }
        return jsonMessage;
      }
    }
    return null;
  }

  /**
   * Convert HDFS audit log to a json object
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
      List<String> listString = extractCompletePath(rawMessage);
      for (int i = 0; i < listString.size(); i++) {
        listString.set(i,listString.get(i).replace(':','/'));
        listString.set(i,listString.get(i).replaceFirst("=", "\":\""));
      }
      String[] partString =
          listString.subList(4, listString.size() - 1).toArray(new String[0]);
      String message = "\"" + StringUtils
          .join("\",\"",partString) + "\"";
      message = message.replace("auth/","auth\":\"");
      message = message.replace("via\",","via\":");
      jsonObject = new JSONObject('{' + message + '}');
      return jsonObject;
    } catch (JSONException e) {
      LOG.warn("[JSON] Audit log format is irregular: " + rawMessage, e);
    } catch (ArrayIndexOutOfBoundsException e) {
      LOG.warn("[INDEX] Audit log format is irregular: " + rawMessage, e);
    } catch (Throwable e) {
      LOG.error("Process hdfs audit log fail!", e);
    }
    return jsonObject;
  }

  /**
   * Extract complete path from audit log, no matter what kind of special character path contains
   */
  private static List<String> extractCompletePath(String rawMessage)
      throws ArrayIndexOutOfBoundsException {
    List<String> result;
    String[] s1 = rawMessage.split("src=");
    String[] s2 = s1[1].split("dst=");
    String[] s3 = s2[1].split("perm=");
    result = new ArrayList<>(Arrays.asList(s1[0].split("[ \t]")));
    result.add("src=" + s2[0].trim());
    result.add("dst=" + s3[0].trim());
    List<String> tmpList = Arrays.asList(("perm=" + s3[1].trim()).split("[ \t]"));
    result.addAll(tmpList);
    // Ignore "callContext" info in audit log
    result.remove(result.size() -1);
    return result;
  }
}