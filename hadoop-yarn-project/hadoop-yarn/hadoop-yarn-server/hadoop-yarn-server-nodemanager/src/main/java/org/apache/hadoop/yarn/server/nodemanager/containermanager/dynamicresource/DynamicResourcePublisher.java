package org.apache.hadoop.yarn.server.nodemanager.containermanager.dynamicresource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerUpdateRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.client.NMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.server.api.CollectorNodemanagerProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReportNewCollectorInfoRequest;
import org.apache.hadoop.yarn.server.api.records.AppCollectorData;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DynamicResourcePublisher extends CompositeService {

  private static final Logger LOG =
      LoggerFactory.getLogger(DynamicResourcePublisher.class);

  private Dispatcher dispatcher;

  private Context context;
  private String port;
  private final YarnRPC rpc;

  private final Map<ApplicationId, AppCollectorData> appToDataMap;

  private final Map<ApplicationId, Token> tokenMap;

  private volatile CollectorNodemanagerProtocol nmCollectorService;

  public DynamicResourcePublisher(Context context) {
    super(DynamicResourcePublisher.class.getName());
    this.context = context;
    port = context.getConf().get(YarnConfiguration.AMRM_PROXY_ADDRESS,
        YarnConfiguration.DEFAULT_AMRM_PROXY_ADDRESS).split(":")[1];
    appToDataMap = new ConcurrentHashMap<>();
    tokenMap = new ConcurrentHashMap<>();
    rpc = YarnRPC.create(context.getConf());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    dispatcher = createDispatcher();
    dispatcher
        .register(DynamicResourceEventType.class, new ForwardingEventHandler());
    addIfService(dispatcher);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
  }

  protected AsyncDispatcher createDispatcher() {
    return new AsyncDispatcher("Dynamic Resource dispatcher");
  }

  private final class ForwardingEventHandler
      implements EventHandler<DynamicResourceEvent> {

    @Override
    public void handle(DynamicResourceEvent event) {
      try {
        handleDynamicResourceEvent(event);
      } catch (IOException | InterruptedException e) {
        LOG.info(e.toString());
      } catch (YarnException e) {
        LOG.info(e.toString());
      }
    }
  }

  public void publishDynamicResourceEvent(DynamicResourceEvent event) {
    dispatcher.getEventHandler().handle(event);
  }

  protected void handleDynamicResourceEvent(DynamicResourceEvent event)
      throws IOException, InterruptedException, YarnException {
    switch (event.getType()) {
      case PUBLISH_UPDATE_CONTAINER_REQUEST:
        publish(event.getApplicationId(), event.getUpdateContainerRequest());
        break;
      case EXECUTE_INCREASE_RESOURCE:
        increaseResource(event.getApplicationId(), event.getUpdatedContainer());
      case STOP_APPLICATION:
        removeApplication(event.getApplicationId());
        break;
      default:
        LOG.error("Unknown DynamicResourceEvent type: " + event.getType());
    }
  }

  private void publish(ApplicationId appId,
      UpdateContainerRequest updateContainerRequest)
      throws IOException, YarnException {
    if (appToDataMap.containsKey(appId)) {
      AppCollectorData appData = appToDataMap.get(appId);
      if (appData.getCollectorToken() == null) {
        LOG.info(appId
            + " doesn't have AMRMProxy token, skip this update container request.");
        return;
      }
      UserGroupInformation user = UserGroupInformation.createRemoteUser("yarn");
      String nodeId = appData.getCollectorAddr().split(":")[0];
      InetSocketAddress addr = NetUtils.createSocketAddr(nodeId + ":" + port);
      Token<AMRMTokenIdentifier> token = ConverterUtils.convertFromYarn(appData.getCollectorToken(), (Text) null);
      token.setService(new Text(addr.getAddress().getHostAddress() + ":" + addr.getPort()));
      user.addToken(token);

      ApplicationMasterProtocol rmClient =
          user.doAs(new PrivilegedAction<ApplicationMasterProtocol>() {
            @Override
            public ApplicationMasterProtocol run() {
              return (ApplicationMasterProtocol) rpc
                  .getProxy(ApplicationMasterProtocol.class, addr,
                      context.getConf());
            }
          });
      LOG.info("Publish update container request: " + updateContainerRequest);
      AllocateRequest allocateRequest = AllocateRequest.newBuilder()
          .updateRequests(Arrays.asList(updateContainerRequest)).build();
      rmClient.allocate(allocateRequest);
    } else {
      LOG.info("Doesn't contain appId: " + appId);
    }
  }

  private void increaseResource(ApplicationId appId, UpdatedContainer updatedContainer)
      throws IOException, YarnException {
    LOG.info(
        "Update container resource, appid: " + appId + " target container: "
            + updatedContainer);
    UserGroupInformation user = UserGroupInformation.createRemoteUser(
        updatedContainer.getContainer().getId().getApplicationAttemptId()
            .toString());
    InetSocketAddress cmAddr =
        NetUtils.createSocketAddr(updatedContainer.getContainer().getNodeId().toString());
    Token<NMTokenIdentifier> nmToken =
        ConverterUtils.convertFromYarn(context.getNMTokenSecretManager()
            .generateNMToken(context.getApplications().get(appId).getUser(),
                updatedContainer.getContainer()).getToken(), cmAddr);
    user.addToken(nmToken);
    ContainerManagementProtocol proxy = NMProxy
        .createNMProxy(getConfig(), ContainerManagementProtocol.class, user,
            rpc, cmAddr);
    List<org.apache.hadoop.yarn.api.records.Token> increaseTokens = new ArrayList<>();
    increaseTokens.add(updatedContainer.getContainer().getContainerToken());
    proxy.updateContainer(ContainerUpdateRequest.newInstance(increaseTokens));
  }

  private void removeApplication(ApplicationId appId) {
    appToDataMap.remove(appId);
    tokenMap.remove(appId);
  }

  public void stopApplication(ApplicationId appId) {
    dispatcher.getEventHandler().handle(
        new DynamicResourceEvent(DynamicResourceEventType.STOP_APPLICATION,
            appId));
  }

  public void updateAMRMProxyToken(ApplicationId appId, Token t) {
    LOG.info("Update AMRMProxy token for " + appId);
    tokenMap.put(appId, t);
    if (appToDataMap.containsKey(appId)) {
      org.apache.hadoop.yarn.api.records.Token newToken =
          org.apache.hadoop.yarn.api.records.Token
              .newInstance(t.getIdentifier(), t.getKind().toString(),
                  t.getPassword(), t.getService().toString());

      AppCollectorData appData = appToDataMap.get(appId);
      if (!appData.getCollectorToken().equals(newToken)) {
        LOG.info("Update " + appId + " AMRMProxy token for AppCollectorData");
        appData.setCollectorToken(newToken);
        ReportNewCollectorInfoRequest request = ReportNewCollectorInfoRequest
            .newInstance(appId, appData.getCollectorAddr(),
                appData.getCollectorToken());
        try {
          getNMCollectorService().reportNewCollectorInfo(request);
        } catch (YarnException e) {
          e.printStackTrace();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public org.apache.hadoop.yarn.api.records.Token getToken(
      ApplicationId appId) {
    if (!tokenMap.containsKey(appId))
      return null;
    Token t = tokenMap.get(appId);
    return org.apache.hadoop.yarn.api.records.Token
        .newInstance(t.getIdentifier(), t.getKind().toString(), t.getPassword(),
            t.getService().toString());
  }

  public void setAppCollectorData(ApplicationId appId, AppCollectorData data) {
    appToDataMap.put(appId, data);
    LOG.info("Set app collector for "+ appId + " data content: " + data.toString());
  }

  protected CollectorNodemanagerProtocol getNMCollectorService() {
    if (nmCollectorService == null) {
      synchronized (this) {
        if (nmCollectorService == null) {
          Configuration conf = getConfig();
          InetSocketAddress nmCollectorServiceAddress = conf.getSocketAddr(
              YarnConfiguration.NM_BIND_HOST,
              YarnConfiguration.NM_COLLECTOR_SERVICE_ADDRESS,
              YarnConfiguration.DEFAULT_NM_COLLECTOR_SERVICE_ADDRESS,
              YarnConfiguration.DEFAULT_NM_COLLECTOR_SERVICE_PORT);
          LOG.info("nmCollectorServiceAddress: " + nmCollectorServiceAddress);
          final YarnRPC rpc = YarnRPC.create(conf);

          // TODO Security settings.
          nmCollectorService = (CollectorNodemanagerProtocol) rpc.getProxy(
              CollectorNodemanagerProtocol.class,
              nmCollectorServiceAddress, conf);
        }
      }
    }
    return nmCollectorService;
  }
}
