package org.apache.hadoop.yarn.server.applicationhistoryservice;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.service.AbstractService;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.yarn.server.metrics.AppAttemptMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ContainerMetricsConstants;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;

public class ApplicationHistoryManagerOnHBase extends AbstractService
    implements ApplicationHistoryManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(ApplicationHistoryManagerOnHBase.class);

  private String readerAddress;
  private String serverHttpAddress;
  private final String PREFIX = "/ws/v2/timeline/clusters/";
  private final String SUFFIX = "?fields=ALL";
  private final String CONTAINER = "/YARN_CONTAINER";
  private final String ATTEMPT = "/YARN_APPLICATION_ATTEMPT";
  private final String ENTITIES = "/entities";
  private String clusterId;

  public ApplicationHistoryManagerOnHBase() {
    super(ApplicationHistoryManagerOnHBase.class.getName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    LOG.info("ApplicationHistoryManagerOnHBase init");
    this.readerAddress = conf.get("yarn.timeline-reader.address");
    this.serverHttpAddress = WebAppUtils.getHttpSchemePrefix(conf) + WebAppUtils
        .getAHSWebAppURLWithoutScheme(conf);
    this.clusterId = YarnConfiguration.getClusterId(conf);
    LOG.info("Reader address: " + readerAddress);
    super.serviceInit(conf);
  }

  @Override
  public ApplicationReport getApplication(ApplicationId appId)
      throws YarnException, IOException {
    Client client = Client.create();
    ClientResponse response = client.resource(
        readerAddress + PREFIX + clusterId + "/apps/" + appId.toString()
            + SUFFIX).type(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    String result = response.getEntity(String.class);
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES,
        false);
    TimelineEntity entity = mapper.readValue(result, TimelineEntity.class);
    return convertToApplicationReport(entity);
  }

  @Override
  public Map<ApplicationId, ApplicationReport> getApplications(long appsNum,
      long appStartedTimeBegin, long appStartedTimeEnd)
      throws YarnException, IOException {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public Map<ApplicationAttemptId, ApplicationAttemptReport> getApplicationAttempts(
      ApplicationId appId) throws YarnException, IOException {
    Client client = Client.create();
    ClientResponse response = client.resource(
        readerAddress + PREFIX + clusterId + "/apps/" + appId.toString()
            + ENTITIES + ATTEMPT + SUFFIX).type(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    String result = response.getEntity(String.class);

    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES,
        false);
    TimelineEntity[] entities =
        mapper.readValue(result, TimelineEntity[].class);

    Map<ApplicationAttemptId, ApplicationAttemptReport> appAttempts =
        new LinkedHashMap<>();
    for (TimelineEntity entity : entities) {
      ApplicationAttemptReport appAttempt =
          convertToApplicationAttemptReport(entity);
      appAttempts.put(appAttempt.getApplicationAttemptId(), appAttempt);
    }
    return appAttempts;
  }

  @Override
  public ApplicationAttemptReport getApplicationAttempt(
      ApplicationAttemptId appAttemptId) throws YarnException, IOException {
    Client client = Client.create();
    ClientResponse response = client.resource(
        readerAddress + PREFIX + clusterId + "/apps/" + appAttemptId
            .getApplicationId().toString() + ENTITIES + ATTEMPT + "/"
            + appAttemptId.toString() + SUFFIX).type(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    String result = response.getEntity(String.class);

    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES,
        false);
    TimelineEntity entity = mapper.readValue(result, TimelineEntity.class);
    return convertToApplicationAttemptReport(entity);
  }

  @Override
  public ContainerReport getContainer(ContainerId containerId)
      throws YarnException, IOException {
    ApplicationId appId =
        containerId.getApplicationAttemptId().getApplicationId();
    String user = getApplication(appId).getUser();

    Client client = Client.create();
    ClientResponse response = client.resource(
        readerAddress + PREFIX + clusterId + "/apps/" + containerId
            .getApplicationAttemptId().getApplicationId().toString() + ENTITIES
            + CONTAINER + "/" + containerId.toString() + SUFFIX)
        .type(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    String result = response.getEntity(String.class);

    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES,
        false);
    TimelineEntity entity = mapper.readValue(result, TimelineEntity.class);
    return convertToContainerReport(entity, serverHttpAddress, user);
  }

  @Override
  public ContainerReport getAMContainer(ApplicationAttemptId appAttemptId)
      throws YarnException, IOException {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public Map<ContainerId, ContainerReport> getContainers(
      ApplicationAttemptId appAttemptId) throws YarnException, IOException {
    ApplicationId appId = appAttemptId.getApplicationId();
    String user = getApplication(appId).getUser();

    Client client = Client.create();
    ClientResponse response = client.resource(
        readerAddress + PREFIX + clusterId + "/apps/" + appAttemptId
            .getApplicationId().toString() + ENTITIES + CONTAINER + SUFFIX)
        .type(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    String result = response.getEntity(String.class);

    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES,
        false);
    TimelineEntity[] entities =
        mapper.readValue(result, TimelineEntity[].class);

    Map<ContainerId, ContainerReport> containers = new LinkedHashMap<>();
    for (TimelineEntity entity : entities) {
      ContainerReport container =
          convertToContainerReport(entity, serverHttpAddress, user);
      containers.put(container.getContainerId(), container);
    }
    return containers;
  }

  public static ApplicationReport convertToApplicationReport(
      TimelineEntity entity) {
    String user = null;
    String queue = null;
    String name = null;
    String type = null;
    boolean unmanagedApplication = false;
    long createdTime = 0;
    long finishedTime = 0;
    long launchedTime = 0;
    float progress = 0.0f;
    int applicationPriority = 0;
    ApplicationAttemptId latestApplicationAttemptId = null;
    String diagnosticsInfo = null;
    FinalApplicationStatus finalStatus = FinalApplicationStatus.UNDEFINED;
    YarnApplicationState state = YarnApplicationState.ACCEPTED;
    ApplicationResourceUsageReport appResources = null;
    Set<String> appTags = null;
    Map<ApplicationAccessType, String> appViewACLs = new HashMap<>();
    String appNodeLabelExpression = null;
    String amNodeLabelExpression = null;
    Map<String, Object> entityInfo = entity.getInfo();
    if (entityInfo != null) {
      if (entityInfo
          .containsKey(ApplicationMetricsConstants.USER_ENTITY_INFO)) {
        user = entityInfo.get(ApplicationMetricsConstants.USER_ENTITY_INFO)
            .toString();
      }
      if (entityInfo
          .containsKey(ApplicationMetricsConstants.APP_VIEW_ACLS_ENTITY_INFO)) {
        String appViewACLsStr = entityInfo
            .get(ApplicationMetricsConstants.APP_VIEW_ACLS_ENTITY_INFO)
            .toString();
        if (appViewACLsStr.length() > 0) {
          appViewACLs.put(ApplicationAccessType.VIEW_APP, appViewACLsStr);
        }
      }
      if (entityInfo
          .containsKey(ApplicationMetricsConstants.QUEUE_ENTITY_INFO)) {
        queue = entityInfo.get(ApplicationMetricsConstants.QUEUE_ENTITY_INFO)
            .toString();
      }
      if (entityInfo
          .containsKey(ApplicationMetricsConstants.NAME_ENTITY_INFO)) {
        name = entityInfo.get(ApplicationMetricsConstants.NAME_ENTITY_INFO)
            .toString();
      }
      if (entityInfo
          .containsKey(ApplicationMetricsConstants.TYPE_ENTITY_INFO)) {
        type = entityInfo.get(ApplicationMetricsConstants.TYPE_ENTITY_INFO)
            .toString();
      }
      if (entityInfo.containsKey(
          ApplicationMetricsConstants.UNMANAGED_APPLICATION_ENTITY_INFO)) {
        unmanagedApplication = Boolean.parseBoolean(entityInfo
            .get(ApplicationMetricsConstants.UNMANAGED_APPLICATION_ENTITY_INFO)
            .toString());
      }
      if (entityInfo
          .containsKey(ApplicationMetricsConstants.APPLICATION_PRIORITY_INFO)) {
        applicationPriority = Integer.parseInt(entityInfo
            .get(ApplicationMetricsConstants.APPLICATION_PRIORITY_INFO)
            .toString());
      }
      if (entityInfo
          .containsKey(ApplicationMetricsConstants.APP_NODE_LABEL_EXPRESSION)) {
        appNodeLabelExpression = entityInfo
            .get(ApplicationMetricsConstants.APP_NODE_LABEL_EXPRESSION)
            .toString();
      }
      if (entityInfo
          .containsKey(ApplicationMetricsConstants.AM_NODE_LABEL_EXPRESSION)) {
        amNodeLabelExpression =
            entityInfo.get(ApplicationMetricsConstants.AM_NODE_LABEL_EXPRESSION)
                .toString();
      }
      if (entityInfo.containsKey(ApplicationMetricsConstants.APP_TAGS_INFO)) {
        appTags = new HashSet<>();
        Object obj = entityInfo.get(ApplicationMetricsConstants.APP_TAGS_INFO);
        if (obj != null && obj instanceof Collection<?>) {
          for (Object o : (Collection<?>) obj) {
            if (o != null) {
              appTags.add(o.toString());
            }
          }
        }
      }
      if (entityInfo.containsKey(
          ApplicationMetricsConstants.LATEST_APP_ATTEMPT_EVENT_INFO)) {
        latestApplicationAttemptId = ApplicationAttemptId.fromString(entityInfo
            .get(ApplicationMetricsConstants.LATEST_APP_ATTEMPT_EVENT_INFO)
            .toString());
      }
      if (entityInfo.containsKey(
          ApplicationMetricsConstants.DIAGNOSTICS_INFO_EVENT_INFO)) {
        diagnosticsInfo = entityInfo
            .get(ApplicationMetricsConstants.DIAGNOSTICS_INFO_EVENT_INFO)
            .toString();
      }
      if (entityInfo
          .containsKey(ApplicationMetricsConstants.FINAL_STATUS_EVENT_INFO)) {
        finalStatus = FinalApplicationStatus.valueOf(
            entityInfo.get(ApplicationMetricsConstants.FINAL_STATUS_EVENT_INFO)
                .toString());
      }
      if (entityInfo
          .containsKey(ApplicationMetricsConstants.STATE_EVENT_INFO)) {
        state = YarnApplicationState.valueOf(
            entityInfo.get(ApplicationMetricsConstants.STATE_EVENT_INFO)
                .toString());
      }
    }
    Set<TimelineMetric> metricsInfo = entity.getMetrics();
    if (metricsInfo != null) {
      Map<String, Long> resourceSecondMap = new HashMap<>();
      Map<String, Long> preemptedResourceSecondMap = new HashMap<>();
      long vcoreSeconds = 0, memorySeconds = 0, preemptedMemorySeconds = 0,
          preemptedVcoreSeconds = 0;
      Iterator<Number> iter = null;
      for (TimelineMetric metric : metricsInfo) {
        switch (metric.getId()) {
          case ApplicationMetricsConstants.APP_CPU_METRICS:
            iter = metric.getValues().values().iterator();
            if (iter.hasNext()) {
              vcoreSeconds = ((Number) iter.next()).longValue();
            }
            break;
          case ApplicationMetricsConstants.APP_MEM_METRICS:
            iter = metric.getValues().values().iterator();
            if (iter.hasNext()) {
              memorySeconds = ((Number) iter.next()).longValue();
            }
            break;
          case ApplicationMetricsConstants.APP_MEM_PREEMPT_METRICS:
            iter = metric.getValues().values().iterator();
            if (iter.hasNext()) {
              preemptedMemorySeconds = ((Number) iter.next()).longValue();
            }
            break;
          case ApplicationMetricsConstants.APP_CPU_PREEMPT_METRICS:
            iter = metric.getValues().values().iterator();
            if (iter.hasNext()) {
              preemptedVcoreSeconds = ((Number) iter.next()).longValue();
            }
            break;
          default:
            ;
        }
      }
      resourceSecondMap
          .put(ResourceInformation.MEMORY_MB.getName(), memorySeconds);
      resourceSecondMap.put(ResourceInformation.VCORES.getName(), vcoreSeconds);
      preemptedResourceSecondMap
          .put(ResourceInformation.MEMORY_MB.getName(), preemptedMemorySeconds);
      preemptedResourceSecondMap
          .put(ResourceInformation.VCORES.getName(), preemptedVcoreSeconds);
      appResources = ApplicationResourceUsageReport
          .newInstance(0, 0, null, null, null, resourceSecondMap, 0, 0,
              preemptedResourceSecondMap);
    }
    NavigableSet<TimelineEvent> events = entity.getEvents();
    long updatedTimeStamp = 0L;
    if (events != null) {
      for (TimelineEvent event : events) {
        if (event.getId()
            .equals(ApplicationMetricsConstants.CREATED_EVENT_TYPE)) {
          createdTime = event.getTimestamp();
        } else if (event.getId()
            .equals(ApplicationMetricsConstants.UPDATED_EVENT_TYPE)) {
          if (event.getTimestamp() > updatedTimeStamp) {
            updatedTimeStamp = event.getTimestamp();
          } else {
            continue;
          }
          Map<String, Object> eventInfo = event.getInfo();
          if (eventInfo == null) {
            continue;
          }
          applicationPriority = Integer.parseInt(eventInfo
              .get(ApplicationMetricsConstants.APPLICATION_PRIORITY_INFO)
              .toString());
          queue = eventInfo.get(ApplicationMetricsConstants.QUEUE_ENTITY_INFO)
              .toString();
        } else if (event.getId()
            .equals(ApplicationMetricsConstants.STATE_UPDATED_EVENT_TYPE)) {
          Map<String, Object> eventInfo = event.getInfo();
          if (eventInfo == null) {
            continue;
          }
          if (eventInfo
              .containsKey(ApplicationMetricsConstants.STATE_EVENT_INFO)) {
            if (!isFinalState(state)) {
              state = YarnApplicationState.valueOf(
                  eventInfo.get(ApplicationMetricsConstants.STATE_EVENT_INFO)
                      .toString());
            }
          }
        } else if (event.getId()
            .equals(ApplicationMetricsConstants.FINISHED_EVENT_TYPE)) {
          progress = 1.0F;
          finishedTime = event.getTimestamp();
        } else if (event.getId()
            .equals(ApplicationMetricsConstants.LAUNCHED_EVENT_TYPE)) {
          launchedTime = event.getTimestamp();
        }
      }
    }
    return ApplicationReport
        .newInstance(ApplicationId.fromString(entity.getId()),
            latestApplicationAttemptId, user, queue, name, null, -1, null,
            state, diagnosticsInfo, null, createdTime, launchedTime,
            finishedTime, finalStatus, appResources, null, progress, type, null,
            appTags, unmanagedApplication,
            Priority.newInstance(applicationPriority), appNodeLabelExpression,
            amNodeLabelExpression);
  }

  private static ApplicationAttemptReport convertToApplicationAttemptReport(
      TimelineEntity entity) {
    String host = null;
    int rpcPort = -1;
    ContainerId amContainerId = null;
    String trackingUrl = null;
    String originalTrackingUrl = null;
    String diagnosticsInfo = null;
    YarnApplicationAttemptState state = null;

    Map<String, Object> entityInfo = entity.getInfo();
    if (entityInfo != null) {
      if (entityInfo.containsKey(AppAttemptMetricsConstants.HOST_INFO)) {
        host = entityInfo.get(AppAttemptMetricsConstants.HOST_INFO).toString();
      }
      if (entityInfo.containsKey(AppAttemptMetricsConstants.RPC_PORT_INFO)) {
        rpcPort =
            (Integer) entityInfo.get(AppAttemptMetricsConstants.RPC_PORT_INFO);
      }
      if (entityInfo
          .containsKey(AppAttemptMetricsConstants.MASTER_CONTAINER_INFO)) {
        amContainerId = ContainerId.fromString(
            entityInfo.get(AppAttemptMetricsConstants.MASTER_CONTAINER_INFO)
                .toString());
      }
      if (entityInfo
          .containsKey(AppAttemptMetricsConstants.TRACKING_URL_INFO)) {
        trackingUrl =
            entityInfo.get(AppAttemptMetricsConstants.TRACKING_URL_INFO)
                .toString();
      }
      if (entityInfo
          .containsKey(AppAttemptMetricsConstants.ORIGINAL_TRACKING_URL_INFO)) {
        originalTrackingUrl = entityInfo
            .get(AppAttemptMetricsConstants.ORIGINAL_TRACKING_URL_INFO)
            .toString();
      }
      if (entityInfo.containsKey(AppAttemptMetricsConstants.DIAGNOSTICS_INFO)) {
        diagnosticsInfo =
            entityInfo.get(AppAttemptMetricsConstants.DIAGNOSTICS_INFO)
                .toString();
      }
      if (entityInfo.containsKey(AppAttemptMetricsConstants.STATE_INFO)) {
        state = YarnApplicationAttemptState.valueOf(
            entityInfo.get(AppAttemptMetricsConstants.STATE_INFO).toString());
      }
      if (entityInfo
          .containsKey(AppAttemptMetricsConstants.MASTER_CONTAINER_INFO)) {
        amContainerId = ContainerId.fromString(
            entityInfo.get(AppAttemptMetricsConstants.MASTER_CONTAINER_INFO)
                .toString());
      }
    }
    return ApplicationAttemptReport
        .newInstance(ApplicationAttemptId.fromString(entity.getId()), host,
            rpcPort, trackingUrl, originalTrackingUrl, diagnosticsInfo, state,
            amContainerId);
  }

  private static ContainerReport convertToContainerReport(TimelineEntity entity,
      String serverHttpAddress, String user) {
    int allocatedMem = 0;
    int allocatedVcore = 0;
    String allocatedHost = null;
    int allocatedPort = -1;
    int allocatedPriority = 0;
    long createdTime = 0;
    long finishedTime = 0;
    String diagnosticsInfo = null;
    int exitStatus = ContainerExitStatus.INVALID;
    ContainerState state = null;
    String nodeHttpAddress = null;
    Map<String, Object> entityInfo = entity.getInfo();
    if (entityInfo != null) {
      if (entityInfo
          .containsKey(ContainerMetricsConstants.ALLOCATED_MEMORY_INFO)) {
        allocatedMem = (Integer) entityInfo
            .get(ContainerMetricsConstants.ALLOCATED_MEMORY_INFO);
      }
      if (entityInfo
          .containsKey(ContainerMetricsConstants.ALLOCATED_VCORE_INFO)) {
        allocatedVcore = (Integer) entityInfo
            .get(ContainerMetricsConstants.ALLOCATED_VCORE_INFO);
      }
      if (entityInfo
          .containsKey(ContainerMetricsConstants.ALLOCATED_HOST_INFO)) {
        allocatedHost =
            entityInfo.get(ContainerMetricsConstants.ALLOCATED_HOST_INFO)
                .toString();
      }
      if (entityInfo
          .containsKey(ContainerMetricsConstants.ALLOCATED_PORT_INFO)) {
        allocatedPort = (Integer) entityInfo
            .get(ContainerMetricsConstants.ALLOCATED_PORT_INFO);
      }
      if (entityInfo
          .containsKey(ContainerMetricsConstants.ALLOCATED_PRIORITY_INFO)) {
        if (entityInfo.get(
            ContainerMetricsConstants.ALLOCATED_PRIORITY_INFO) instanceof String) {
          allocatedPriority = Integer.parseInt((String) entityInfo
              .get(ContainerMetricsConstants.ALLOCATED_PRIORITY_INFO));
        } else {
          allocatedPriority = (Integer) entityInfo
              .get(ContainerMetricsConstants.ALLOCATED_PRIORITY_INFO);
        }
      }
      if (entityInfo.containsKey(
          ContainerMetricsConstants.ALLOCATED_HOST_HTTP_ADDRESS_INFO)) {
        nodeHttpAddress = (String) entityInfo
            .get(ContainerMetricsConstants.ALLOCATED_HOST_HTTP_ADDRESS_INFO);
        if (!nodeHttpAddress.startsWith("http://")) {
          nodeHttpAddress = "http://" + nodeHttpAddress;
        }
      }
      if (entityInfo.containsKey(ContainerMetricsConstants.DIAGNOSTICS_INFO)) {
        diagnosticsInfo =
            entityInfo.get(ContainerMetricsConstants.DIAGNOSTICS_INFO)
                .toString();
      }
      if (entityInfo.containsKey(ContainerMetricsConstants.EXIT_STATUS_INFO)) {
        exitStatus = (Integer) entityInfo
            .get(ContainerMetricsConstants.EXIT_STATUS_INFO);
      }
      if (entityInfo.containsKey(ContainerMetricsConstants.STATE_INFO)) {
        state = ContainerState.valueOf(
            entityInfo.get(ContainerMetricsConstants.STATE_INFO).toString());
      }
    }

    NavigableSet<TimelineEvent> events = entity.getEvents();
    if (events != null) {
      for (TimelineEvent event : events) {
        if (event.getId()
            .equals(ContainerMetricsConstants.CREATED_EVENT_TYPE)) {
          createdTime = event.getTimestamp();
        } else if (event.getId()
            .equals(ContainerMetricsConstants.FINISHED_EVENT_TYPE)) {
          finishedTime = event.getTimestamp();
        }
      }
    }
    ContainerId containerId = ContainerId.fromString(entity.getId());
    String logUrl = null;
    NodeId allocatedNode = null;
    if (allocatedHost != null) {
      allocatedNode = NodeId.newInstance(allocatedHost, allocatedPort);
      logUrl = WebAppUtils
          .getAggregatedLogURL(serverHttpAddress, allocatedNode.toString(),
              containerId.toString(), containerId.toString(), user);
    }
    return ContainerReport.newInstance(ContainerId.fromString(entity.getId()),
        Resource.newInstance(allocatedMem, allocatedVcore), allocatedNode,
        Priority.newInstance(allocatedPriority), createdTime, finishedTime,
        diagnosticsInfo, logUrl, exitStatus, state, nodeHttpAddress);
  }

  private static boolean isFinalState(YarnApplicationState state) {
    return state == YarnApplicationState.FINISHED
        || state == YarnApplicationState.FAILED
        || state == YarnApplicationState.KILLED;
  }
}
