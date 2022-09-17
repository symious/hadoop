package org.apache.hadoop.yarn.server.nodemanager.containermanager.dynamicresource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.amrmproxy.AMRMProxyApplicationContext;
import org.apache.hadoop.yarn.server.nodemanager.amrmproxy.AbstractRequestInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DynamicResourceRequestInterceptor extends
    AbstractRequestInterceptor {
  private static final Logger LOG =
      LoggerFactory.getLogger(DynamicResourceRequestInterceptor.class);

  private ApplicationAttemptId attemptId;
  private String userName;

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
    this.attemptId = appContext.getApplicationAttemptId();
    this.userName = appContext.getUser();
  }

  @Override
  public RegisterApplicationMasterResponse registerApplicationMaster(
      RegisterApplicationMasterRequest request)
      throws YarnException, IOException {
    RegisterApplicationMasterResponse response =
        getNextInterceptor().registerApplicationMaster(request);
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
    handleIncreaseContainers(response);
    if (response.getUpdateErrors().size() > 0) {
      LOG.info("UpdateErrors: " + response.getUpdateErrors().toString());
    }
    return response;
  }

  public List<UpdateContainerRequest> getUpdateContainerRequests() {
    List<UpdateContainerRequest> toBeUpdated = new ArrayList<>(
        getApplicationContext().getNMCotext().getTobeUpdatedContainers()
            .values());
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
      this.getApplicationContext().getNMCotext().getContainerManager()
          .updateContainer(request);
    }
  }

  @Override
  public void shutdown() {
    getUpdateContainerRequests();
    super.shutdown();
  }
}
