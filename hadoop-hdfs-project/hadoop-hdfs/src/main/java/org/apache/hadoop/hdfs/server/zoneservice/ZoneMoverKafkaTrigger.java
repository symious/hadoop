package org.apache.hadoop.hdfs.server.zoneservice;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ZoneMoverKafkaTrigger extends ZoneMoverTrigger {
  private static final Logger LOG = LoggerFactory.getLogger(ZoneMover.class);

  private final String nameSpace;
  private final Consumer<String, String> consumer;
  private List<Path> monitorPaths;
  private final int consumerThreadsNum;
  private final BlockingQueue<String> pathQueue;

  //Init HDFS audit log kafka consumer
  public ZoneMoverKafkaTrigger(Configuration conf,
      List<Path> paths, URI namenode) {
    final String username =
        conf.get(DFSConfigKeys.DFS_ZONEMOVER_KAFKA_USERNAME);
    final String password =
        conf.get(DFSConfigKeys.DFS_ZONEMOVER_KAFKA_PASSWORD);
    final String bootstrapServers =
        conf.get(DFSConfigKeys.DFS_ZONEMOVER_KAFKA_BOOTSTRAP_SERVERS);
    final String topic =
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
    nameSpace = namenode.getAuthority();
    properties.put("group.id", groupId + "_" + nameSpace);

    properties.setProperty("security.protocol", "SASL_PLAINTEXT");
    properties.setProperty("sasl.mechanism", "PLAIN");
    properties.setProperty("sasl.jaas.config",
        "org.apache.kafka.common.security.plain.PlainLoginModule " +
            "required username=\""+username+"\" password=\""+password+"\";");

    final int queueSize =
        conf.getInt(DFSConfigKeys.DFS_ZONEMOVER_TRIGGER_QUEUE_SIZE_KEY,
            DFSConfigKeys.DFS_ZONEMOVER_TRIGGER_QUEUE_SIZE_DEFAULT);
    pathQueue = new LinkedBlockingQueue<>(queueSize);
    consumerThreadsNum =
        conf.getInt(DFSConfigKeys.DFS_ZONEMOVER_TRIGGER_KAFKA_CONSUMER_THREADS_KEY,
            DFSConfigKeys.DFS_ZONEMOVER_TRIGGER_KAFKA_CONSUMER_THREADS_DEFAULT);
    consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(Collections.singletonList(topic));

    monitorPaths = paths;
    for (int i = 0; i < consumerThreadsNum; ++i) {
      new MonitorThread(this.getClass().getSimpleName() + "_" +
          nameSpace + "_" + i).start();
    }
    LOG.info("ZoneMover trigger for {} has been started!", nameSpace);
  }

  @Override
  public boolean hasNext() {
    return true;
  }

  @Override
  public String getNext() throws InterruptedException {
    return pathQueue.take();
  }

  public void monitorPaths() {
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

          processMessage(message);
        }
      } catch (JSONException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        LOG.info("ZoneMover trigger thread is interrupted!");
        break;
      }
    }
  }

  /**
   * Choose new files from HDFS audit log
   */
  private void processMessage(String message) throws JSONException, InterruptedException {
    if (message.contains("cmd=complete")) {
      JSONObject jsonMessage = message2json(message);
      if (jsonMessage.get("allowed").equals("true")) {
        if (checkPaths(jsonMessage.get("src").toString())) {
          pathQueue.put(jsonMessage.get("src").toString());
          LOG.info("New create file: " +
              jsonMessage.get("src").toString());
        }
      }
    } else if (message.contains("cmd=rename")) {
      JSONObject jsonMessage = message2json(message);
      if (jsonMessage.get("allowed").equals("true")) {
        if (checkPaths(jsonMessage.get("dst").toString())) {
          pathQueue.put(jsonMessage.get("dst").toString());
          LOG.info("New rename file: " +
              jsonMessage.get("dst").toString());
        }
      }
    }
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

  /**
   * Check if the path is under the monitor paths
   * @param curPath path need to be checked
   * @return TRUE means current path is under monitor paths, need to process it
   *         FALSE means current path isn't under monitor paths,
   *         no need to process
   */
  protected boolean checkPaths(String curPath) {
    for (Path path: monitorPaths) {
      if (curPath.startsWith(path.toString())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void updatePaths(List<Path> paths) {
    monitorPaths = paths;
  }

  @Override
  public void shutdown() {
    monitorThread.interrupt();
  }

  class MonitorThread extends Thread {
    public MonitorThread(String name) {
      super(name);
    }

    @Override
    public void run() {
      monitorPaths();
    }
  }
}