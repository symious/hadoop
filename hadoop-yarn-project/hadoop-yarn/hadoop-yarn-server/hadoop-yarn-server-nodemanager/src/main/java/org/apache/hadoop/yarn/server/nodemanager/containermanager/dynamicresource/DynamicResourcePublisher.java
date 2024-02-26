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
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
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
  private final YarnRPC rpc;

  private volatile CollectorNodemanagerProtocol nmCollectorService;

  public DynamicResourcePublisher(Context context) {
    super(DynamicResourcePublisher.class.getName());
    this.context = context;
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
      case EXECUTE_INCREASE_RESOURCE:
        increaseResource(event.getApplicationId(), event.getUpdatedContainer());
      default:
        LOG.error("Unknown DynamicResourceEvent type: " + event.getType());
    }
  }

  private void increaseResource(ApplicationId appId, UpdatedContainer updatedContainer)
      throws IOException, YarnException {
    Application application = context.getApplications().get(appId);
    if (application == null) {
      return;
    }
    LOG.info(
        "Update container resource, appid: " + appId + " target container: "
            + updatedContainer);
    UserGroupInformation user = UserGroupInformation.createRemoteUser(
        updatedContainer.getContainer().getId().getApplicationAttemptId()
            .toString());
    InetSocketAddress cmAddr =
        NetUtils.createSocketAddr(updatedContainer.getContainer().getNodeId().toString());
    Token<NMTokenIdentifier> nmToken = ConverterUtils.convertFromYarn(
        context.getNMTokenSecretManager().generateNMToken(application.getUser(),
            updatedContainer.getContainer()).getToken(), cmAddr);
    user.addToken(nmToken);
    ContainerManagementProtocol proxy = NMProxy
        .createNMProxy(getConfig(), ContainerManagementProtocol.class, user,
            rpc, cmAddr);
    List<org.apache.hadoop.yarn.api.records.Token> increaseTokens = new ArrayList<>();
    increaseTokens.add(updatedContainer.getContainer().getContainerToken());
    proxy.updateContainer(ContainerUpdateRequest.newInstance(increaseTokens));
  }
}
