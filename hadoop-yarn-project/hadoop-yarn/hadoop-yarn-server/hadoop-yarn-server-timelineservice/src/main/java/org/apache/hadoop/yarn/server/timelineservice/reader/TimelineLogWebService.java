package org.apache.hadoop.yarn.server.timelineservice.reader;

import com.google.inject.Singleton;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.urlconnection.HttpURLConnectionFactory;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerFactory;
import org.apache.hadoop.yarn.server.metrics.AppAttemptMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ContainerMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.LogWebServiceMetrics;
import org.apache.hadoop.yarn.server.timelineservice.metrics.TimelineReaderMetrics;
import org.apache.hadoop.yarn.server.webapp.*;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.ForbiddenException;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.Set;

@Singleton
@Path("/ws/v2/applicationlog")
public class TimelineLogWebService implements AppInfoProvider {
  private static final Logger LOG =
      LoggerFactory
          .getLogger(TimelineLogWebService.class);
  private static final String RESOURCE_URI_STR_V2 = "/ws/v2/timeline/";
  private static final String NM_DOWNLOAD_URI_STR = "/ws/v1/node/containers";
  private static final Joiner JOINER = Joiner.on("");
  private static Configuration yarnConf = new YarnConfiguration();
  private static LogAggregationFileControllerFactory factory;
  private static String base;
  private static String defaultClusterid;

  private final LogServlet logServlet;
  private volatile Client webTimelineClient;

  private static final LogWebServiceMetrics METRICS =
      LogWebServiceMetrics.getInstance();

  private static final TimelineReaderMetrics ENTITY_METRICS =
      TimelineReaderMetrics.getInstance();

  @Context
  private ServletContext ctxt;

  static {
    init();
  }

  // initialize all the common resources - order is important
  private static void init() {
    factory = new LogAggregationFileControllerFactory(yarnConf);
    base = JOINER.join(WebAppUtils.getHttpSchemePrefix(yarnConf),
        WebAppUtils.getTimelineReaderWebAppURLWithoutScheme(yarnConf),
        RESOURCE_URI_STR_V2);
    defaultClusterid = yarnConf.get(YarnConfiguration.RM_CLUSTER_ID,
        YarnConfiguration.DEFAULT_RM_CLUSTER_ID);
    LOG.info("Initialized LogWeService with clusterid " + defaultClusterid
        + " for URI: " + base);
  }

  public TimelineLogWebService() {
    this.logServlet = new LogServlet(yarnConf, this);
  }

  private Client createTimelineWebClient() {
    ClientConfig cfg = new DefaultClientConfig();
    cfg.getClasses().add(YarnJacksonJaxbJsonProvider.class);
    Client client = new Client(
        new URLConnectionClientHandler(new HttpURLConnectionFactory() {
          @Override public HttpURLConnection getHttpURLConnection(URL url)
              throws IOException {
            AuthenticatedURL.Token token = new AuthenticatedURL.Token();
            HttpURLConnection conn = null;
            try {
              conn = new AuthenticatedURL().openConnection(url, token);
              LOG.info("LogWeService:Connecetion created.");
            } catch (AuthenticationException e) {
              throw new IOException(e);
            }
            return conn;
          }
        }), cfg);

    return client;
  }

  private void initForReadableEndpoints(HttpServletResponse response) {
    // clear content type
    response.setContentType(null);
  }

  @GET
  @Path("/apps/{appid}/amlogs")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public Response getAMContainerLogsInfo(@Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam(YarnWebServiceParams.APP_ID) String appId,
      @QueryParam(YarnWebServiceParams.NM_ID) String nmId,
      @QueryParam(YarnWebServiceParams.REDIRECTED_FROM_NODE)
      @DefaultValue("false") boolean redirectedFromNode,
      @QueryParam(YarnWebServiceParams.CLUSTER_ID) String clusterId,
      @QueryParam(YarnWebServiceParams.MANUAL_REDIRECTION)
      @DefaultValue("false") boolean manualRedirection) {
    if (clusterId == null || clusterId.isEmpty()) {
      clusterId = getAppToCluster(appId);
      if (clusterId == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity("Cluster id is not found.").build();
      }
    }
    Set<TimelineEntity> appAttemptEntities = getAppAttempts(appId, clusterId);
    TimelineEntity latestAttemptEntity = getLatestId(appAttemptEntities);
    if (latestAttemptEntity == null) {
      Response.ResponseBuilder response = Response.noContent();
      return response.build();
    }
    String amContainerId = (String) latestAttemptEntity.getInfo().
        get(AppAttemptMetricsConstants.MASTER_CONTAINER_INFO);
    if (amContainerId == null) {
      return Response.status(Response.Status.NOT_FOUND)
          .entity("Am container id is not found.").build();
    }
    return getContainerLogsInfo(req, res, amContainerId, nmId, redirectedFromNode, clusterId,
        manualRedirection);
  }

  @GET
  @Path("/apps/{appid}/amlogs/{filename}")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public Response getAMContainerLogFile(@Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam(YarnWebServiceParams.APP_ID) String appId,
      @PathParam(YarnWebServiceParams.CONTAINER_LOG_FILE_NAME) String filename,
      @QueryParam(YarnWebServiceParams.RESPONSE_CONTENT_FORMAT) String format,
      @QueryParam(YarnWebServiceParams.RESPONSE_START)
      @DefaultValue("0") String start,
      @QueryParam(YarnWebServiceParams.RESPONSE_CONTENT_SIZE) String size,
      @QueryParam(YarnWebServiceParams.NM_ID) String nmId,
      @QueryParam(YarnWebServiceParams.REDIRECTED_FROM_NODE)
      @DefaultValue("false") boolean redirectedFromNode,
      @QueryParam(YarnWebServiceParams.CLUSTER_ID) String clusterId,
      @QueryParam(YarnWebServiceParams.MANUAL_REDIRECTION)
      @DefaultValue("false") boolean manualRedirection) {
    if (clusterId == null || clusterId.isEmpty()) {
      clusterId = getAppToCluster(appId);
      if (clusterId == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity("Cluster id is not found").build();
      }
    }
    Set<TimelineEntity> appAttemptEntities = getAppAttempts(appId, clusterId);
    TimelineEntity latestAttemptEntity = getLatestId(appAttemptEntities);
    if (latestAttemptEntity == null) {
      Response.status(Response.Status.NOT_FOUND)
          .entity("LatestAttempt is not found").build();
    }
    String amContainerId = (String) latestAttemptEntity.getInfo().
        get(AppAttemptMetricsConstants.MASTER_CONTAINER_INFO);
    if (amContainerId == null) {
      LOG.info("AM container of " + appId + " is not found.");
      return Response.status(Response.Status.NOT_FOUND)
          .entity("AM container id is not found.").build();
    }
    return getLogs(req, res, amContainerId, filename, format, start, size,
        nmId, redirectedFromNode, clusterId, manualRedirection);
  }

  protected TimelineEntity getLatestId(Set<TimelineEntity> entities) {
    if (entities.size() == 0) {
      return null;
    }
    TimelineEntity tmp = null;
    for (TimelineEntity entity : entities) {
      if (tmp == null) {
        tmp = entity;
        continue;
      }
      if (tmp.compareTo(entity) < 0) {
        tmp = entity;
      }
    }
    return tmp;
  }

  /**
   * Returns log file's name as well as current file size for a container.
   *
   * @param req                HttpServletRequest
   * @param res                HttpServletResponse
   * @param containerIdStr     The container ID
   * @param nmId               The Node Manager NodeId
   * @param redirectedFromNode Whether this is a redirected request from NM
   * @return The log file's name and current file size
   */
  @GET
  @Path("/containers/{containerid}/logs")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public Response getContainerLogsInfo(@Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam(YarnWebServiceParams.CONTAINER_ID) String containerIdStr,
      @QueryParam(YarnWebServiceParams.NM_ID) String nmId,
      @QueryParam(YarnWebServiceParams.REDIRECTED_FROM_NODE)
      @DefaultValue("false") boolean redirectedFromNode,
      @QueryParam(YarnWebServiceParams.CLUSTER_ID) String clusterId,
      @QueryParam(YarnWebServiceParams.MANUAL_REDIRECTION)
      @DefaultValue("false") boolean manualRedirection) {
    initForReadableEndpoints(res);

    WrappedLogMetaRequest.Builder logMetaRequestBuilder =
        LogServlet.createRequestFromContainerId(containerIdStr);

    return logServlet.getContainerLogsInfo(req, logMetaRequestBuilder, nmId,
        redirectedFromNode, clusterId, manualRedirection);
  }

  @Override
  public String getNodeHttpAddress(HttpServletRequest req, String appId,
      String appAttemptId, String containerId, String clusterId) {
    UserGroupInformation callerUGI = LogWebServiceUtils.getUser(req);
    String cId = clusterId != null ? clusterId : defaultClusterid;
    MultivaluedMap<String, String> params = new MultivaluedMapImpl();
    params.add("fields", "INFO");
    String path = JOINER.join("clusters/", cId, "/apps/", appId, "/entities/",
        TimelineEntityType.YARN_CONTAINER.toString(), "/", containerId);
    TimelineEntity conEntity = null;
    try {
      if (callerUGI == null) {
        conEntity =
            getEntity(clusterId, appId, TimelineEntityType.YARN_CONTAINER.toString(),
                containerId);
      } else {
        setUserName(params, callerUGI.getShortUserName());
        conEntity =
            callerUGI.doAs(new PrivilegedExceptionAction<TimelineEntity>() {
              @Override public TimelineEntity run() throws Exception {
                return getEntity(clusterId, appId, TimelineEntityType.YARN_CONTAINER.toString(),
                    containerId);
              }
            });
      }
    } catch (Exception e) {
      LogWebServiceUtils.rewrapAndThrowException(e);
    }
    if (conEntity == null) {
      return null;
    }
    return (String) conEntity.getInfo()
        .get(ContainerMetricsConstants.ALLOCATED_HOST_HTTP_ADDRESS_INFO);
  }

  public Set<TimelineEntity> getAppAttempts(String appId, String clusterId) {
    long startTime = Time.monotonicNow();
    boolean succeeded = false;
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    Set<TimelineEntity> entities = null;
    try {
      TimelineReaderContext context = TimelineReaderWebServicesUtils
          .createTimelineReaderContext(clusterId, null, null, null,
              appId, "YARN_APPLICATION_ATTEMPT", null, null);
      entities = timelineReaderManager.getEntities(context,
          TimelineReaderWebServicesUtils
              .createTimelineEntityFilters(null, (String) null,
                  (String)null, null, null, null,
                  null, null, null, null),
          TimelineReaderWebServicesUtils
              .createTimelineDataToRetrieve(null, null,
                  "INFO", null, null, null));
      succeeded = true;
    } catch (Exception e) {
      handleException(e, "getAppAttempts", startTime, "");
      LOG.error("getAppAttempts error for appId: " + appId, e);
    } finally {
      long latency = Time.monotonicNow() - startTime;
      ENTITY_METRICS.addGetEntitiesLatency(latency, succeeded);
      LOG.info("Processed getAppAttempts for " + appId +
          " (Took " + latency + " ms.)");
    }
    if (entities == null) {
      entities = Collections.emptySet();
    }
    return entities;
  }

  @Override
  public BasicAppInfo getApp(HttpServletRequest req, String appId,
      String clusterId) {
    UserGroupInformation callerUGI = LogWebServiceUtils.getUser(req);

    String cId = clusterId != null ? clusterId : defaultClusterid;
    MultivaluedMap<String, String> params = new MultivaluedMapImpl();
    params.add("fields", "INFO");
    String path = JOINER.join("clusters/", cId, "/apps/", appId);
    TimelineEntity appEntity = null;
    try {
      if (callerUGI == null) {
        appEntity = getAppEntity(clusterId, appId);
      } else {
        setUserName(params, callerUGI.getShortUserName());
        appEntity =
            callerUGI.doAs(new PrivilegedExceptionAction<TimelineEntity>() {
              @Override public TimelineEntity run() throws Exception {
                return getAppEntity(clusterId, appId);
              }
            });
      }
    } catch (Exception e) {
      LogWebServiceUtils.rewrapAndThrowException(e);
    }

    if (appEntity == null) {
      return null;
    }
    String appOwner = (String) appEntity.getInfo()
        .get(ApplicationMetricsConstants.USER_ENTITY_INFO);
    String state = (String) appEntity.getInfo()
        .get(ApplicationMetricsConstants.STATE_EVENT_INFO);
    YarnApplicationState appState = YarnApplicationState.valueOf(state);
    return new BasicAppInfo(appState, appOwner);
  }

  /**
   * Returns the contents of a container's log file in plain text.
   *
   * @param req                HttpServletRequest
   * @param res                HttpServletResponse
   * @param containerIdStr     The container ID
   * @param filename           The name of the log file
   * @param format             The content type
   * @param size               the size of the log file
   * @param nmId               The Node Manager NodeId
   * @param redirectedFromNode Whether this is the redirect request from NM
   * @return The contents of the container's log file
   */
  @GET
  @Path("/containers/{containerid}/logs/{filename}")
  @Produces({ MediaType.TEXT_PLAIN })
  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  public Response getContainerLogFile(
      @Context HttpServletRequest req, @Context HttpServletResponse res,
      @PathParam(YarnWebServiceParams.CONTAINER_ID) String containerIdStr,
      @PathParam(YarnWebServiceParams.CONTAINER_LOG_FILE_NAME) String filename,
      @QueryParam(YarnWebServiceParams.RESPONSE_CONTENT_FORMAT) String format,
      @QueryParam(YarnWebServiceParams.RESPONSE_START)
      @DefaultValue("0") String start,
      @QueryParam(YarnWebServiceParams.RESPONSE_CONTENT_SIZE) String size,
      @QueryParam(YarnWebServiceParams.NM_ID) String nmId,
      @QueryParam(YarnWebServiceParams.REDIRECTED_FROM_NODE)
          boolean redirectedFromNode,
      @QueryParam(YarnWebServiceParams.CLUSTER_ID) String clusterId,
      @QueryParam(YarnWebServiceParams.MANUAL_REDIRECTION)
      @DefaultValue("false") boolean manualRedirection) {
    return getLogs(req, res, containerIdStr, filename, format, start, size,
        nmId, redirectedFromNode, clusterId, manualRedirection);
  }

  //TODO: YARN-4993: Refactory ContainersLogsBlock, AggregatedLogsBlock and
  //      container log webservice introduced in AHS to minimize
  //      the duplication.
  @GET
  @Path("/containerlogs/{containerid}/{filename}")
  @Produces({ MediaType.TEXT_PLAIN + "; " + JettyUtils.UTF_8 })
  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  public Response getLogs(@Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam(YarnWebServiceParams.CONTAINER_ID) String containerIdStr,
      @PathParam(YarnWebServiceParams.CONTAINER_LOG_FILE_NAME) String filename,
      @QueryParam(YarnWebServiceParams.RESPONSE_CONTENT_FORMAT) String format,
      @QueryParam(YarnWebServiceParams.RESPONSE_START)
      @DefaultValue("0") String start,
      @QueryParam(YarnWebServiceParams.RESPONSE_CONTENT_SIZE) String size,
      @QueryParam(YarnWebServiceParams.NM_ID) String nmId,
      @QueryParam(YarnWebServiceParams.REDIRECTED_FROM_NODE)
      @DefaultValue("false") boolean redirectedFromNode,
      @QueryParam(YarnWebServiceParams.CLUSTER_ID) String clusterId,
      @QueryParam(YarnWebServiceParams.MANUAL_REDIRECTION)
      @DefaultValue("false") boolean manualRedirection) {
    initForReadableEndpoints(res);
    if (clusterId == null || clusterId.isEmpty()) {
      clusterId = getAppToCluster(
          ContainerId.fromString(containerIdStr).getApplicationAttemptId()
              .getApplicationId().toString());
    }
    long startTime = Time.monotonicNow();
    Response response = logServlet.getLogFile(req, containerIdStr, filename, format, start,
        size, nmId, redirectedFromNode, clusterId, manualRedirection);
    long latency = Time.monotonicNow() - startTime;
    METRICS.addGetLogsLatency(latency);
    return response;
  }

  public Set<TimelineEntity> getEntities(String clusterId, String appId)
      throws IOException {
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    Set<TimelineEntity> entities = null;
    try {
      TimelineReaderContext context = TimelineReaderWebServicesUtils
          .createTimelineReaderContext(clusterId, null, null, null,
              appId, "YARN_APPLICATION_ATTEMPT", null, null);
      entities = timelineReaderManager.getEntities(context,
          TimelineReaderWebServicesUtils
              .createTimelineEntityFilters(null, (String) null,
                  (String)null, null, null, null,
                  null, null, null, null),
          TimelineReaderWebServicesUtils
              .createTimelineDataToRetrieve(null, null,
                  "INFO", null, null, null));
    } catch (Exception e) {
      e.printStackTrace();
    }
    return entities;
  }

  public TimelineEntity getEntity(String clusterId, String appId, String entityType,
      String entityId) {
    long startTime = Time.monotonicNow();
    boolean succeeded = false;
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    TimelineEntity entity = null;
    try {
      entity = timelineReaderManager.getEntity(
          TimelineReaderWebServicesUtils.createTimelineReaderContext(
              clusterId, null, null, null, appId, entityType,
              null, entityId),
          TimelineReaderWebServicesUtils.createTimelineDataToRetrieve(
              null, null, "INFO", null,
              null, null));
      succeeded = true;
    } catch (Exception e) {
      handleException(e, "get entityType", startTime, "");
    } finally {
      long latency = Time.monotonicNow() - startTime;
      ENTITY_METRICS.addGetEntitiesLatency(latency, succeeded);
      LOG.info("Processed getEntity(entityType:" + entityType + ", entityId: "
          + entityId + ") for " + appId + " (Took " + latency + " ms.)");
    }
    return entity;
  }

  public TimelineEntity getAppEntity(String clusterId, String appId) {
    long startTime = Time.monotonicNow();
    boolean succeeded = false;
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    TimelineEntity entity = null;
    try {
      entity = timelineReaderManager.getEntity(
          TimelineReaderWebServicesUtils.createTimelineReaderContext(
              clusterId, null, null, null, appId,
              TimelineEntityType.YARN_APPLICATION.toString(), null, null),
          TimelineReaderWebServicesUtils.createTimelineDataToRetrieve(
              null, null, "INFO", null,
              null, null));
      succeeded = true;
    } catch (Exception e) {
      handleException(e, "getApp", startTime, "");
    } finally {
      long latency = Time.monotonicNow() - startTime;
      ENTITY_METRICS.addGetEntitiesLatency(latency, succeeded);
      LOG.info("Processed getApp for " + appId +
          " (Took " + latency + " ms.)");
    }
    return entity;
  }

  @VisibleForTesting protected String getString(String path,
      MultivaluedMap<String, String> params) throws IOException {
    ClientResponse resp =
        getClient().resource(base).path(path)
            .accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON)
            .get(ClientResponse.class);
    if (resp == null
        || resp.getStatusInfo().getStatusCode() != ClientResponse.Status.OK
        .getStatusCode()) {
      String msg =
          "Response from the timeline reader server is " + ((resp == null) ?
              "null" :
              "not successful," + " HTTP error code: " + resp.getStatus()
                  + ", Server response:\n" + resp.getEntity(String.class));
      LOG.error(msg);
      throw new IOException(msg);
    }
    String res = resp.getEntity(String.class);
    return res;
  }

  private Client getClient() {
    if (webTimelineClient == null) {
      synchronized (org.apache.hadoop.yarn.server.webapp.LogWebService.class) {
        if (webTimelineClient == null) {
          webTimelineClient = createTimelineWebClient();
        }
      }
    }
    return webTimelineClient;
  }

  /**
   * Set user.name in non-secure mode to delegate to next rest call.
   */
  private void setUserName(MultivaluedMap<String, String> params, String user) {
    if (!UserGroupInformation.isSecurityEnabled()) {
      params.add("user.name", user);
    }
  }

  public String getAppToCluster(String appId) {
    long startTime = Time.monotonicNow();
    boolean succeeded = false;
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    Set<String> result = null;
    try {
      result = timelineReaderManager.getEntityTypes(
          TimelineReaderWebServicesUtils
              .createTimelineReaderContext(null, null, null, null, appId,
                  TimelineEntityType.YARN_CLUSTER.toString(), null, null));
      succeeded = true;
    } catch (Exception e) {
      handleException(e, "getAppToCluster", startTime, "");
    } finally {
      long latency = Time.monotonicNow() - startTime;
      ENTITY_METRICS.addGetEntitiesLatency(latency, succeeded);
      LOG.info("Processed getAppToCluster for " + appId +
          " (Took " + latency + " ms.)");
    }
    if (result == null || result.size() == 0) {
      return null;
    }
    return (String) result.toArray()[0];
  }

  private TimelineReaderManager getTimelineReaderManager() {
    return (TimelineReaderManager)
        ctxt.getAttribute(TimelineReaderServer.TIMELINE_READER_MANAGER_ATTR);
  }

  private static void handleException(Exception e, String url, long startTime,
      String invalidNumMsg) throws BadRequestException,
      WebApplicationException {
    long endTime = Time.monotonicNow();
    LOG.info("Processed " + url + " but encountered exception (Took " +
        (endTime - startTime) + " ms.)");
    if (e instanceof NumberFormatException) {
      throw new BadRequestException(invalidNumMsg + " is not a numeric value.");
    } else if (e instanceof IllegalArgumentException) {
      throw new BadRequestException(e.getMessage() == null ?
          "Requested Invalid Field." : e.getMessage());
    } else if (e instanceof NotFoundException) {
      throw (NotFoundException)e;
    } else if (e instanceof TimelineParseException) {
      throw new BadRequestException(e.getMessage() == null ?
          "Filter Parsing failed." : e.getMessage());
    } else if (e instanceof BadRequestException) {
      throw (BadRequestException)e;
    } else if (e instanceof ForbiddenException) {
      throw (ForbiddenException) e;
    } else {
      throw new WebApplicationException(e,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }
}
