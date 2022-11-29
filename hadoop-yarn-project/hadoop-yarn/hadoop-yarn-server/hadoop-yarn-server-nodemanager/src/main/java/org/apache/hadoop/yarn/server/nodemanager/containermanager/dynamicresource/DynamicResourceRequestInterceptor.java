package org.apache.hadoop.yarn.server.nodemanager.containermanager.dynamicresource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.amrmproxy.AMRMProxyApplicationContext;
import org.apache.hadoop.yarn.server.nodemanager.amrmproxy.AbstractRequestInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DynamicResourceRequestInterceptor extends
    AbstractRequestInterceptor {
  private static final Logger LOG =
      LoggerFactory.getLogger(DynamicResourceRequestInterceptor.class);

  private ApplicationAttemptId attemptId;
  private String userName;
  private List<UpdateContainerRequest> pendingUpdateRequest;

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
    this.pendingUpdateRequest = Collections.synchronizedList(new ArrayList<>());
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
    if (request.getUpdateRequests().size() > 0) {
      pendingUpdateRequest.addAll(request.getUpdateRequests());
      return AllocateResponse.newBuilder().build();
    }
    synchronized (pendingUpdateRequest) {
      request.getUpdateRequests().addAll(pendingUpdateRequest);
      pendingUpdateRequest.clear();
    }
    AllocateResponse response = getNextInterceptor().allocate(request);
    handleIncreaseContainers(response);
    if (response.getUpdateErrors().size() > 0) {
      LOG.info("UpdateErrors: " + response.getUpdateErrors().toString());
    }
    return response;
  }

  public void handleIncreaseContainers(AllocateResponse response)
      throws IOException, YarnException {
    List<UpdatedContainer> updatedContainers = new ArrayList();
    updatedContainers.addAll(response.getUpdatedContainers());
    response.getUpdatedContainers().clear();
    for (UpdatedContainer uc : updatedContainers) {
      if (uc.getUpdateType().equals(ContainerUpdateType.INCREASE_RESOURCE)) {
        getApplicationContext().getNMCotext().getDynamicResourcePublisher()
            .publishDynamicResourceEvent(new DynamicResourceEvent(
                DynamicResourceEventType.EXECUTE_INCREASE_RESOURCE,
                attemptId.getApplicationId(), uc));
      }
    }
  }

  @Override
  public void shutdown() {
    super.shutdown();
  }
}
