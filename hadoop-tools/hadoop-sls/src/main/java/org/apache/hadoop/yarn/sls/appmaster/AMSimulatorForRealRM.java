/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.sls.appmaster;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.sls.SLSRunnerForRealRM;
import org.apache.hadoop.yarn.sls.scheduler.ContainerSimulator;
import org.apache.hadoop.yarn.sls.scheduler.TaskRunner;
import org.apache.hadoop.yarn.sls.utils.SLSUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Logger;

@Private
@Unstable
public abstract class AMSimulatorForRealRM extends TaskRunner.Task {
  // resource manager
  // protected ResourceManager rm;
  // main
  protected SLSRunnerForRealRM se;
  // application
  protected ApplicationId appId;
  protected ApplicationAttemptId appAttemptId;
  protected String oldAppId; // jobId from the jobhistory file
  // record factory
  protected final static RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  // response queue
  protected final BlockingQueue<AllocateResponse> responseQueue;
  protected int RESPONSE_ID = 0;
  // user name
  protected String user;
  // queue name
  protected String queue;
  // am type
  protected String amtype;
  // job start/end time
  protected long traceStartTimeMS;
  protected long traceFinishTimeMS;
  protected long simulateStartTimeMS;
  protected long simulateFinishTimeMS;
  // whether tracked in Metrics
  protected boolean isTracked;
  // progress
  protected int totalContainers;
  protected int finishedContainers;

  protected int id;

  protected final Logger LOG = Logger.getLogger(AMSimulator.class);

  private static final long AM_STATE_WAIT_TIMEOUT_MS = 60 * 60 * 1000;

  protected YarnClient yarnClient;

  protected ApplicationMasterProtocol amRMClient;

  protected UserGroupInformation ugi = null;

  protected UserGroupInformation clientUGI = null;

  protected Configuration conf = null;
  protected Configuration amConf = null;

  protected boolean isAMRegistered = false;

  public AMSimulatorForRealRM() {
    this.responseQueue = new LinkedBlockingQueue<AllocateResponse>();
  }

  public void init(int id, int heartbeatInterval,
      List<ContainerSimulator> containerList, final Configuration conf, SLSRunnerForRealRM se,
      long traceStartTime, long traceFinishTime, String user, String queue,
      boolean isTracked, String oldAppId) {
    super.init(traceStartTime, traceStartTime + 1000000L * heartbeatInterval,
        heartbeatInterval);
    this.id = id;
    this.user = user;
    this.se = se;
    this.user = user;
    this.queue = queue;
    this.oldAppId = oldAppId;
    this.isTracked = isTracked;
    this.traceStartTimeMS = traceStartTime;
    this.traceFinishTimeMS = traceFinishTime;

    this.conf = conf;

    this.amConf = new Configuration(this.conf);
    amConf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        SaslRpcServer.AuthMethod.TOKEN.toString());

    clientUGI =  UserGroupInformation.createRemoteUser(user);
    try {
      clientUGI.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() {
          yarnClient = YarnClient.createYarnClient();
          yarnClient.init(conf);
          return null;
        }
      });
    } catch (Exception e) {
      LOG.error("create yarn client failed", e);
    }
  }

  public int getId()
  {
    return this.id;
  }
  /**
   * register with RM
   */
  @Override
  public void firstStep() throws Exception {
    simulateStartTimeMS =
        System.currentTimeMillis() - SLSRunnerForRealRM.getAMRunner().getStartTimeMS();

    // submit application, waiting until ACCEPTED
    try{
      submitApp();
    }catch (Exception e){
      e.printStackTrace();
      SLSRunnerForRealRM.decreaseRemainingApps();
      throw e;
    }
  }

  @Override
  public void middleStep() throws Exception {
    // process responses in the queue
    long begin = System.currentTimeMillis();

    try{
      // register application master
      if(this.isAMRegistered == false){
        registerAM();
      }else{
        processResponseQueue();

        // send out request
        sendContainerRequest();

        // check whether finish
        checkStop();
      }
    }catch (Exception e){
      e.printStackTrace();
      SLSRunnerForRealRM.decreaseRemainingApps();
      throw e;
    }




  }

  @Override
  public void lastStep() throws Exception {
    try{
      LOG.info(MessageFormat.format("Application {0} is shutting down.", appId));
      // unregister application master
      final FinishApplicationMasterRequest finishAMRequest =
          recordFactory.newRecordInstance(FinishApplicationMasterRequest.class);
      finishAMRequest.setFinalApplicationStatus(FinalApplicationStatus.SUCCEEDED);

      ugi.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          amRMClient.finishApplicationMaster(finishAMRequest);
          return null;
        }
      });
      // Monitor the application for end state
      ApplicationReport appReport =
          monitorApplication(appId, EnumSet.of(YarnApplicationState.KILLED,
              YarnApplicationState.FAILED, YarnApplicationState.FINISHED));
      LOG.info("app finsihed:" + appId + " last state:"
          + appReport.getFinalApplicationStatus());
      simulateFinishTimeMS =
          System.currentTimeMillis() - SLSRunnerForRealRM.getAMRunner().getStartTimeMS();
      if (yarnClient != null)
        yarnClient.stop();
      if (amRMClient != null)
        RPC.stopProxy(this.amRMClient);
      LOG.info("app :" + appId + " all rpc client is stop");
      SLSRunnerForRealRM.decreaseRemainingApps();
    }catch (Exception e){
      e.printStackTrace();
      SLSRunnerForRealRM.decreaseRemainingApps();
      throw e;
    }

  }

  protected ResourceRequest createResourceRequest(Resource resource,
      String host, int priority, int numContainers) {
    ResourceRequest request =
        recordFactory.newRecordInstance(ResourceRequest.class);
    request.setCapability(resource);
    request.setResourceName(host);
    request.setNumContainers(numContainers);
    Priority prio = recordFactory.newRecordInstance(Priority.class);
    prio.setPriority(priority);
    request.setPriority(prio);
    return request;
  }

  protected AllocateRequest createAllocateRequest(List<ResourceRequest> ask,
      List<ContainerId> toRelease) {
    AllocateRequest allocateRequest =
        recordFactory.newRecordInstance(AllocateRequest.class);
    allocateRequest.setResponseId(RESPONSE_ID++);
    allocateRequest.setAskList(ask);
    allocateRequest.setReleaseList(toRelease);
    return allocateRequest;
  }

  protected AllocateRequest createAllocateRequest(List<ResourceRequest> ask) {
    return createAllocateRequest(ask, new ArrayList<ContainerId>());
  }

  protected abstract void processResponseQueue() throws Exception;

  protected abstract void sendContainerRequest() throws Exception;

  protected abstract void checkStop();

  private void submitApp()
      throws YarnException, InterruptedException, IOException {
    // ask for new application
    clientUGI.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() {
        yarnClient.start();
        return null;
      }
    });
    if(LOG.isDebugEnabled())
      LOG.debug("AM_ID:" + id + " ,yarnclient started.");
    final ApplicationSubmissionContext appSubContext =
        yarnClient.createApplication().getApplicationSubmissionContext();
    appId = appSubContext.getApplicationId();
    if(LOG.isDebugEnabled())
      LOG.debug(
          "AM_ID:" + id + " , create application successfully, appID:" + appId);
    // submit the application
    appSubContext.setMaxAppAttempts(1);
    appSubContext.setQueue(queue);
    appSubContext.setPriority(Priority.newInstance(0));
    ContainerLaunchContext conLauContext =
        Records.newRecord(ContainerLaunchContext.class);
    conLauContext
        .setApplicationACLs(new HashMap<ApplicationAccessType, String>());
    conLauContext.setCommands(new ArrayList<String>());
    conLauContext.setEnvironment(new HashMap<String, String>());
    conLauContext.setLocalResources(new HashMap<String, LocalResource>());
    conLauContext.setServiceData(new HashMap<String, ByteBuffer>());
    appSubContext.setAMContainerSpec(conLauContext);
    appSubContext.setUnmanagedAM(true);

    clientUGI.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws YarnException, IOException {
        yarnClient.submitApplication(appSubContext);
        return null;
      }
    });

    LOG.info(MessageFormat.format("Submit a new application {0}", appId));

    // waiting until application ACCEPTED
    ApplicationReport appReport =
        monitorApplication(appId,
            EnumSet.of(YarnApplicationState.ACCEPTED,
                YarnApplicationState.KILLED, YarnApplicationState.FAILED,
                YarnApplicationState.FINISHED));

    LOG.info(MessageFormat.format("application {0}  is ACCEPTED ", appId));
    if (appReport.getYarnApplicationState() == YarnApplicationState.ACCEPTED) {
      // Monitor the application attempt to wait for launch state
      ApplicationAttemptReport attemptReport =
          monitorCurrentAppAttempt(appId, YarnApplicationAttemptState.LAUNCHED);
      ApplicationAttemptId attemptId = attemptReport.getApplicationAttemptId();
      LOG.info("Launching AM with application attempt id " + attemptId);
      // generate credentials ugi
      Token<AMRMTokenIdentifier> token =
          yarnClient.getAMRMToken(attemptId.getApplicationId());
      ugi = UserGroupInformation.createRemoteUser(user);
      Credentials credentials = new Credentials();
      credentials.addToken(token.getService(), token);
      ugi.addCredentials(credentials);
      new Thread(new AMRMClientCreater(), appId + "-AMRMClientCreater").start();
    }
  }

  public class AMRMClientCreater implements  Runnable{

    @Override public void run() {
      ApplicationMasterProtocol client =  null;
      while(true){
        try {
          if(LOG.isDebugEnabled())
            LOG.debug(MessageFormat.format("application {0}  create am proxy rpc beginning", appId));
          client =
              ugi.doAs(new PrivilegedExceptionAction<ApplicationMasterProtocol>() {
                @Override
                public ApplicationMasterProtocol run() throws Exception {
                  return ClientRMProxy.createRMProxy(amConf,
                      ApplicationMasterProtocol.class);
                }
              });
          amRMClient = client;
          if(LOG.isDebugEnabled())
            LOG.debug(MessageFormat.format("application {0}  create am proxy rpc successfully", appId));
          break;
        } catch (IOException e) {
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  /**
   * Monitor the submitted application for completion. Kill application if time
   * expires.
   *
   * @param appId Application Id of application to be monitored
   * @return true if application completed successfully
   * @throws YarnException
   * @throws IOException
   */
  private ApplicationReport monitorApplication(ApplicationId appId,
      Set<YarnApplicationState> finalState) throws YarnException, IOException {

    while (true) {
      // Check app status every 1 second.
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.debug("Thread sleep in monitoring loop interrupted");
      }

      // Get application report for the appId we are interested in
      ApplicationReport report = yarnClient.getApplicationReport(appId);

      YarnApplicationState state = report.getYarnApplicationState();
      if (finalState.contains(state)) {
        LOG.info("Got application report from ASM for" + ", appId="
            + appId.getId() + ", appAttemptId="
            + report.getCurrentApplicationAttemptId() + ", clientToAMToken="
            + report.getClientToAMToken() + ", appDiagnostics="
            + report.getDiagnostics() + ", appMasterHost=" + report.getHost()
            + ", appQueue=" + report.getQueue() + ", appMasterRpcPort="
            + report.getRpcPort() + ", appStartTime=" + report.getStartTime()
            + ", yarnAppState=" + report.getYarnApplicationState().toString()
            + ", distributedFinalState="
            + report.getFinalApplicationStatus().toString() + ", appTrackingUrl="
            + report.getTrackingUrl() + ", appUser=" + report.getUser());

        return report;
      }
    }
  }

  private ApplicationAttemptReport monitorCurrentAppAttempt(ApplicationId appId,
      YarnApplicationAttemptState attemptState)
      throws YarnException, IOException {
    long startTime = System.currentTimeMillis();
    ApplicationAttemptId attemptId = null;
    while (true) {
      if (attemptId == null) {
        attemptId = yarnClient.getApplicationReport(appId)
            .getCurrentApplicationAttemptId();
      }
      ApplicationAttemptReport attemptReport = null;
      if (attemptId != null) {
        try{
          attemptReport = yarnClient.getApplicationAttemptReport(attemptId);
        }catch (Exception e){

        }

        if (attemptReport != null && attemptState
            .equals(attemptReport.getYarnApplicationAttemptState())) {
          return attemptReport;
        }
      }
      if(attemptReport != null){
        LOG.info("Current attempt state of " + appId + " is "
            + (attemptReport == null ? " N/A "
            : attemptReport.getYarnApplicationAttemptState())
            + ", waiting for current attempt to reach " + attemptState);
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting for current attempt of " + appId
            + " to reach " + attemptState);
      }
      if (System.currentTimeMillis() - startTime > AM_STATE_WAIT_TIMEOUT_MS) {
        String errmsg = "Timeout for waiting current attempt of " + appId
            + " to reach " + attemptState;
        LOG.error(errmsg);
        throw new RuntimeException(errmsg);
      }
    }
  }

  private void registerAM()
      throws YarnException, IOException, InterruptedException {
    // register application master
    if(amRMClient == null) return;

    final RegisterApplicationMasterRequest amRegisterRequest =
        Records.newRecord(RegisterApplicationMasterRequest.class);
    amRegisterRequest.setHost("localhost");
    amRegisterRequest.setRpcPort(-1);
    amRegisterRequest.setTrackingUrl("localhost:-1");
    LOG.info(MessageFormat
        .format("beging Register the application master for application {0}", appId));
    ugi.doAs(
        new PrivilegedExceptionAction<RegisterApplicationMasterResponse>() {
          @Override
          public RegisterApplicationMasterResponse run() throws Exception {
            return amRMClient.registerApplicationMaster(amRegisterRequest);
          }
        });
    LOG.info(MessageFormat
        .format("Register the application master for application {0}", appId));
    this.isAMRegistered = true;
  }

  protected List<ResourceRequest> packageRequests(
      List<ContainerSimulator> csList, int priority) {
    // create requests
    Map<String, ResourceRequest> rackLocalRequestMap =
        new HashMap<String, ResourceRequest>();
    Map<String, ResourceRequest> nodeLocalRequestMap =
        new HashMap<String, ResourceRequest>();
    ResourceRequest anyRequest = null;
    for (ContainerSimulator cs : csList) {
      String rackHostNames[] = SLSUtils.getRackHostName(cs.getHostname());
      // check rack local
      String rackname = rackHostNames[0];
      if (rackLocalRequestMap.containsKey(rackname)) {
        rackLocalRequestMap.get(rackname).setNumContainers(
            rackLocalRequestMap.get(rackname).getNumContainers() + 1);
      } else {
        ResourceRequest request =
            createResourceRequest(cs.getResource(), rackname, priority, 1);
        rackLocalRequestMap.put(rackname, request);
      }
      // check node local
      String hostname = rackHostNames[1];
      if (nodeLocalRequestMap.containsKey(hostname)) {
        nodeLocalRequestMap.get(hostname).setNumContainers(
            nodeLocalRequestMap.get(hostname).getNumContainers() + 1);
      } else {
        ResourceRequest request =
            createResourceRequest(cs.getResource(), hostname, priority, 1);
        nodeLocalRequestMap.put(hostname, request);
      }
      // any
      if (anyRequest == null) {
        anyRequest = createResourceRequest(cs.getResource(),
            ResourceRequest.ANY, priority, 1);
      } else {
        anyRequest.setNumContainers(anyRequest.getNumContainers() + 1);
      }
    }
    List<ResourceRequest> ask = new ArrayList<ResourceRequest>();
    ask.addAll(nodeLocalRequestMap.values());
    ask.addAll(rackLocalRequestMap.values());
    if (anyRequest != null) {
      ask.add(anyRequest);
    }
    return ask;
  }

  public String getQueue() {
    return queue;
  }

  public String getAMType() {
    return amtype;
  }

  public long getDuration() {
    return simulateFinishTimeMS - simulateStartTimeMS;
  }

  public int getNumTasks() {
    return totalContainers;
  }
}
