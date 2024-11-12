package org.apache.hadoop.yarn.server.timelineservice.reader;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.thirdparty.com.google.common.cache.Cache;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND;
import static javax.servlet.http.HttpServletResponse.SC_OK;

public class TimelineAppStateChecker extends AbstractService {
  private static final Logger LOG =
      LoggerFactory.getLogger(TimelineAppStateChecker.class);

  private static Pattern pattern =
      Pattern.compile("\\{\"state\":\"([A-Z]*)\"\\}");

  private Cache<Object, Object> cache;
  private ExecutorService executorService;
  private TimelineWriter timelineWriter;
  private BlockingQueue<TimelineEntity> queue;
  private Thread thread;
  private ConcurrentHashMap<String, Object> inFlight;
  private String[] routerAddress;
  private UserGroupInformation ugi;
  private ScheduledExecutorService writerFlusher;

  public TimelineAppStateChecker() {
    super(TimelineAppStateChecker.class.getName());
  }

  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    this.cache = CacheBuilder.newBuilder().expireAfterWrite(conf.getLong(
        YarnConfiguration.TIMELINE_SERVICE_CHECK_APP_STATE_INTERVAL_SECONDS,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_CHECK_APP_STATE_INTERVAL_SECONDS),
        TimeUnit.SECONDS).build();
    this.executorService = HadoopExecutors.newFixedThreadPool(10);
    this.timelineWriter = createTimelineWriter(conf);
    this.timelineWriter.init(conf);
    this.writerFlusher = Executors.newSingleThreadScheduledExecutor();
    this.queue = new LinkedBlockingQueue();
    this.inFlight = new ConcurrentHashMap();
    this.routerAddress =
        conf.getStrings("yarn.timeline-service.reader.router.address");
    this.ugi = UserGroupInformation.getCurrentUser();
  }

  protected void serviceStart() throws Exception {
    super.serviceStart();
    if (timelineWriter != null) {
      timelineWriter.start();
    }
    thread = new Thread(createThread());
    thread.start();
    writerFlusher
        .scheduleAtFixedRate(new WriterFlushTask(timelineWriter), 30, 30,
            TimeUnit.SECONDS);
  }

  Runnable createThread() {
    return new Runnable() {
      @Override
      public void run() {
        while (!Thread.currentThread().isInterrupted()) {
          try {
            TimelineEntity entity = queue.take();
            if (inFlight.contains(entity.getId())) {
              continue;
            }
            inFlight.put(entity.getId(), System.currentTimeMillis());
            executorService.submit(new StateChecker(entity));
          } catch (InterruptedException e) {
            LOG.info("TimelineAppStateChecker thread interrupted", e);
          }
        }
      }
    };
  }

  public void record(TimelineEntity entity) {
    if ((entity.getInfo()
        .containsKey(ApplicationMetricsConstants.STATE_EVENT_INFO) && !entity
        .getInfo().get(ApplicationMetricsConstants.STATE_EVENT_INFO)
        .equals("RUNNING")) || !entity.getInfo()
        .containsKey(ApplicationMetricsConstants.STATE_EVENT_INFO)) {
      // If there is no state or the state is not running, skip.
      return;
    }
    if (cache.getIfPresent(entity.getId()) != null) {
      return;
    }
    try {
      queue.put(entity);
      cache.put(entity.getId(), System.currentTimeMillis());
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private TimelineWriter createTimelineWriter(final Configuration conf) {
    String timelineWriterClassName =
        conf.get(YarnConfiguration.TIMELINE_SERVICE_WRITER_CLASS,
            YarnConfiguration.DEFAULT_TIMELINE_SERVICE_WRITER_CLASS);

    try {
      Class<?> timelineWriterClazz = Class.forName(timelineWriterClassName);
      if (TimelineWriter.class.isAssignableFrom(timelineWriterClazz)) {
        return (TimelineWriter) ReflectionUtils
            .newInstance(timelineWriterClazz, conf);
      } else {
        throw new YarnRuntimeException(
            "Class: " + timelineWriterClassName + " not instance of "
                + TimelineWriter.class.getCanonicalName());
      }
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException(
          "Could not instantiate TimelineWriter: " + timelineWriterClassName,
          e);
    }
  }

  private class StateChecker implements Runnable {

    private TimelineEntity entity;

    public StateChecker(TimelineEntity entity) {
      this.entity = entity;
    }

    @Override
    public void run() {
      String expectedState = getAppStateFromRouter(entity.getId());
      if (expectedState == null) {
        inFlight.remove(entity.getId());
        return;
      }
      String actualState = (String) entity.getInfo()
          .get(ApplicationMetricsConstants.STATE_EVENT_INFO);
      if (!expectedState.equals(actualState)) {
        try {
          LOG.info(
              "Will correct appId: " + entity.getId() + " state. expected: "
                  + expectedState + " actual: " + actualState);
          writeExpectedState(expectedState);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      inFlight.remove(entity.getId());
    }

    private void writeExpectedState(String expectedState) throws IOException {
      TimelineCollectorContext context = new TimelineCollectorContext();
      String[] splits = ((String) entity.getInfo().get("FROM_ID")).split("!");
      context.setClusterId(splits[0]);
      context.setUserId(splits[1]);
      context.setFlowName(splits[2]);
      context.setFlowRunId(Long.parseLong(splits[3]));
      context.setAppId(entity.getId());
      TimelineEntities entities = new TimelineEntities();
      entity
          .addInfo(ApplicationMetricsConstants.STATE_EVENT_INFO, expectedState);
      entities.addEntity(entity);
      timelineWriter.write(context, entities, ugi);
    }

    private String getAppStateFromRouter(String appId) {
      String state = null;
      boolean allNotFound = true;
      for (String routerAddr : routerAddress) {
        ClientResponse response = invokeRouterAppStateAPI(routerAddr, appId);
        if (response.getStatus() == SC_OK) {
          String result = response.getEntity(String.class);
          Matcher matcher = pattern.matcher(result);
          if (matcher.find()) {
            state = matcher.group(1);
            return state;
          }
        } else if (response.getStatus() != SC_NOT_FOUND) {
          allNotFound = false;
        }
      }
      if (allNotFound) {
        state = "FINISHED";
      }
      return state;
    }

    private ClientResponse invokeRouterAppStateAPI(String webAddr,
        String appId) {
      String state = null;
      Client client = Client.create();
      ClientResponse response = client.resource(webAddr).path("ws/v1/cluster")
          .path("/apps/" + appId + "/state").type(MediaType.APPLICATION_JSON)
          .get(ClientResponse.class);
      return response;
    }
  }

  private static class WriterFlushTask implements Runnable {
    private final TimelineWriter writer;

    public WriterFlushTask(TimelineWriter writer) {
      this.writer = writer;
    }

    public void run() {
      try {
        writer.flush();
      } catch (Throwable th) {
        // we need to handle all exceptions or subsequent execution may be
        // suppressed
        LOG.error("exception during timeline writer flush!", th);
      }
    }
  }
}
