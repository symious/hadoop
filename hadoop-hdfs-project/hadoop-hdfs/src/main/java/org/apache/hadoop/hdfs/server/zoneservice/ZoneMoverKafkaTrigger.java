package org.apache.hadoop.hdfs.server.zoneservice;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.util.StringUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ZoneMoverKafkaTrigger extends ZoneMoverTrigger {
  private static final Logger LOG = LoggerFactory.getLogger(ZoneMover.class);

  private final Consumer<String, String> consumer;
  private List<Path> monitorPaths;
  private final Thread thread;
  private final BlockingQueue<String> pathQueue;
  private final List<String> hostIpList = new ArrayList<>();

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
    String nameSpace = namenode.getAuthority();
    properties.put("group.id", groupId + "_" + nameSpace);

    properties.setProperty("security.protocol", "SASL_PLAINTEXT");
    properties.setProperty("sasl.mechanism", "PLAIN");
    properties.setProperty("sasl.jaas.config",
        "org.apache.kafka.common.security.plain.PlainLoginModule " +
            "required username=\""+username+"\" password=\""+password+"\";");

    filterHostIp(conf, namenode);

    final int queueSize =
        conf.getInt(DFSConfigKeys.DFS_ZONEMOVER_TRIGGER_QUEUE_SIZE_KEY,
            DFSConfigKeys.DFS_ZONEMOVER_TRIGGER_QUEUE_SIZE_DEFAULT);
    pathQueue = new LinkedBlockingQueue<>(queueSize);

    consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(Collections.singletonList(topic));

    monitorPaths = paths;
    thread = new monitorThread(this.getClass().getName() + "_" +
        nameSpace);
    thread.start();
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
        for (ConsumerRecord<String, String> record : records) {
          JSONObject jsonObject = new JSONObject(record.value());
          String hostname = (new JSONObject(jsonObject.get("host")
              .toString())).get("name").toString();
          //Filter the record doesn't belong to this namespace
          if (!hostIpList.contains(hostname)) { continue; }
          String message = jsonObject.get("message").toString();
          if (message.contains("cmd=complete")) {
            JSONObject jsonMessage = processMessage(message);
            if (jsonMessage.get("allowed").equals("true")) {
              if (checkPaths(jsonMessage.get("src").toString())) {
                pathQueue.put(jsonMessage.get("src").toString());
                LOG.debug("New create file: " +
                    jsonMessage.get("src").toString());
              }
            }
          } else if (message.contains("cmd=rename")) {
            JSONObject jsonMessage = processMessage(message);
            if (jsonMessage.get("allowed").equals("true")) {
              if (checkPaths(jsonMessage.get("dst").toString())) {
                pathQueue.put(jsonMessage.get("dst").toString());
                LOG.debug("New create file: " +
                    jsonMessage.get("dst").toString());
              }
            }
          }
        }
      } catch (JSONException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        break;
      }
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
  protected static JSONObject processMessage(String rawMessage) {
    JSONObject jsonObject = new JSONObject();
    try {
      List<String> listString = Arrays
          .asList(rawMessage.split("[ \t]"));
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

  /**
   * Get the hostname and IP of this namespace's nn&ob, update hostIpList
   * @param conf      Configuration of hdfs
   * @param namenode  the URI of namespace
   */
  private void filterHostIp(Configuration conf, URI namenode) {
    try {
      Map<String, Map<String, InetSocketAddress>>
          addressList = DFSUtil.getNNServiceRpcAddresses(conf);
      String ns = namenode.getAuthority();
      if (addressList.get(ns) == null) {
        LOG.error("RPC addresses for namespace is null!");
        throw new IllegalArgumentException("Null RPC address");
      }
      List<InetSocketAddress> hostList =
          new ArrayList<>(addressList.get(ns).values());
      for (InetSocketAddress address : hostList) {
        String hostIp = address.toString().split(":")[0];
        hostIpList.addAll(Arrays.asList(hostIp.split("/")));
      }
    } catch (IOException e) {
      LOG.error("Cannot load rpc addresses from configuration!");
    }
  }

  @Override
  public void updatePaths(List<Path> paths) {
    monitorPaths = paths;
  }

  @Override
  public void shutdown() {
    thread.interrupt();
  }

  class monitorThread extends Thread {
    public monitorThread(String name) {
      super(name);
    }

    @Override
    public void run() {
      monitorPaths();
    }
  }
}