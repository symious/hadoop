package org.apache.hadoop.yarn.server.nodemanager.containermanager.dynamicresource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.NMProxy;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.amrmproxy.AMRMProxyApplicationContext;
import org.apache.hadoop.yarn.server.nodemanager.amrmproxy.AbstractRequestInterceptor;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DynamicResourceRequestInterceptor extends
    AbstractRequestInterceptor {
  private static final Logger LOG =
      LoggerFactory.getLogger(DynamicResourceRequestInterceptor.class);

  private Token token;
  private ContainerManagementProtocol proxy;
  private NodeId nodeId;
  private String userName;
  private YarnRPC rpc;
  private InetSocketAddress addr;

  @Override
  public void init(AMRMProxyApplicationContext appContext) {
    super.init(appContext);
    LOG.info("Initializing Dynamic Resource Request Interceptor");

    Configuration conf = appContext.getConf();
    if (conf == null) {
      conf = getConf();
    } else {
      setConf(conf);
    }
    this.nodeId = appContext.getNMCotext().getNodeId();
    this.rpc = YarnRPC.create(conf);
    this.addr = new InetSocketAddress(nodeId.getHost(), nodeId.getPort());
    this.userName = appContext.getUser();
  }

  @Override
  public RegisterApplicationMasterResponse registerApplicationMaster(
      RegisterApplicationMasterRequest request)
      throws YarnException, IOException {
    RegisterApplicationMasterResponse response =
        getNextInterceptor().registerApplicationMaster(request);
    updateNMTokenAndProxy(response.getNMTokensFromPreviousAttempts());
    return response;
  }

  @Override
  public FinishApplicationMasterResponse finishApplicationMaster(
      FinishApplicationMasterRequest request)
      throws YarnException, IOException {
    return getNextInterceptor().finishApplicationMaster(request);
  }

  @Override
  public AllocateResponse allocate(AllocateRequest request)
      throws YarnException, IOException {
    request.getUpdateRequests().addAll(getUpdateContainerRequests());

    AllocateResponse response = getNextInterceptor().allocate(request);

    updateNMTokenAndProxy(response.getNMTokens());
    handleIncreaseContainers(response);
    return response;
  }

  public List<UpdateContainerRequest> getUpdateContainerRequests() {
    List<UpdateContainerRequest> toBeUpdated = new ArrayList<>(
        getApplicationContext().getNMCotext().getTobeUpdatedContainers()
            .values());
    ApplicationAttemptId attemptId = getApplicationContext().getApplicationAttemptId();
    Iterator<UpdateContainerRequest> iter = toBeUpdated.iterator();
    while(iter.hasNext()) {
      UpdateContainerRequest ucr = iter.next();
      if (attemptId.equals(ucr.getContainerId().getApplicationAttemptId())) {
        getApplicationContext().getNMCotext().getTobeUpdatedContainers()
            .remove(ucr.getContainerId());
      } else {
        iter.remove();
      }
    }
    return toBeUpdated;
  }

  private void updateNMTokenAndProxy(List<NMToken> nmTokens) {
    Token newToken = null;
    for (NMToken token : nmTokens) {
      String nId = token.getNodeId().toString();
      if (nId.equals(nodeId.toString())) {
        newToken = token.getToken();
      }
    }
    if (newToken  == null) {
      return;
    } else {
      if (token == null) {
        token = newToken;
        UserGroupInformation user =
            UserGroupInformation.createRemoteUser(userName);
        org.apache.hadoop.security.token.Token<NMTokenIdentifier> nmToken =
            ConverterUtils.convertFromYarn(token, addr);
        user.addToken(nmToken);
        proxy = NMProxy
            .createNMProxy(getConf(), ContainerManagementProtocol.class, user,
                rpc, addr);
        return;
      } else {
        if (!token.getIdentifier().equals(newToken.getIdentifier())) {
          rpc.stopProxy(proxy, getConf());
          UserGroupInformation user =
              UserGroupInformation.createRemoteUser(userName);
          org.apache.hadoop.security.token.Token<NMTokenIdentifier> nmToken =
              ConverterUtils.convertFromYarn(token, addr);
          user.addToken(nmToken);
          proxy = NMProxy
              .createNMProxy(getConf(), ContainerManagementProtocol.class, user,
                  rpc, addr);
        }
      }
    }
  }

  public void handleIncreaseContainers(AllocateResponse response)
      throws IOException, YarnException {
    List<Token> increaseTokens = new ArrayList<>();
    List<UpdatedContainer> updatedContainers = new ArrayList();
    updatedContainers.addAll(response.getUpdatedContainers());
    response.getUpdatedContainers().clear();
    for (UpdatedContainer uc : updatedContainers) {
      if (uc.getUpdateType().equals(ContainerUpdateType.INCREASE_RESOURCE)) {
        increaseTokens.add(uc.getContainer().getContainerToken());
      }
    }
    if (increaseTokens.size() > 0) {
      LOG.info("Increase containers: " + increaseTokens);
      ContainerUpdateRequest request =
          ContainerUpdateRequest.newInstance(increaseTokens);
      proxy.updateContainer(request);
    }
  }

  @Override
  public void shutdown() {
    getUpdateContainerRequests();
    super.shutdown();
  }
}
