package org.apache.hadoop.yarn.sls;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.AMRMClientUtils;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.NMTokenCache;
import org.apache.hadoop.yarn.client.api.impl.ContainerManagementProtocolProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationMasterNotRegisteredException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.sls.conf.SLSConfiguration;
import org.apache.hadoop.yarn.sls.scheduler.TaskRunner;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MockAMRunner extends Configured implements Tool {

  private static TaskRunner runner = new TaskRunner();
  private String rmClientAddress;
  private String rmScheAddress;
  private int appNum;
  private int poolSize;
  private int heartbeatInterval;

  private boolean isHA;
  private String rmClient1;
  private String rmClient2;
  private String rmSche1;
  private String rmSche2;

  private Map<Integer, String> labelSet = new HashMap<>();
  private List<Integer> weightsList = new ArrayList<>();
  private static Random random = new Random();

  private ContainerManagementProtocolProxy cmProxy;
  private NMTokenCache nmTokenCache = NMTokenCache.getSingleton();

  public final static Logger LOG = LoggerFactory.getLogger(MockAMRunner.class);

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new YarnConfiguration(), new MockAMRunner(), args);
  }

  public void start() {
    cmProxy = new ContainerManagementProtocolProxy(getConf(), nmTokenCache);

    if (isHA) {
      getConf().setBoolean("yarn.resourcemanager.ha.enabled", true);
      getConf().set("yarn.resourcemanager.ha.rm-ids", "rm1,rm2");
      getConf().set("yarn.resourcemanager.scheduler.address.rm1", rmSche1);
      getConf().set("yarn.resourcemanager.scheduler.address.rm2", rmSche2);
      getConf().set("yarn.resourcemanager.address.rm1", rmClient1);
      getConf().set("yarn.resourcemanager.address.rm2", rmClient2);
    } else {
      getConf().set("yarn.resourcemanager.address", rmClientAddress);
      getConf()
          .set("yarn.resourcemanager.resource-tracker.address", rmScheAddress);
      getConf().setBoolean("yarn.resourcemanager.ha.enabled", false);
    }

    if (heartbeatInterval > 0) {
      getConf()
          .setInt(SLSConfiguration.AM_HEARTBEAT_INTERVAL_MS, heartbeatInterval);
    }

    runner.setQueueSize(poolSize);
    runner.start();
    Random random = new Random();
    ExecutorService executorService = Executors.newFixedThreadPool(100);
    for (int i = 0; i < appNum; i++) {
      int finalI = i;
      executorService.submit(new Runnable() {
        @Override
        public void run() {
          try {
            MockAMRunner.MockAM mockAM =
                new MockAMRunner.MockAM(getConf(), finalI, MockAMRunner.this, labelSet, weightsList);
            int heartbeatInterval = getConf()
                .getInt(SLSConfiguration.AM_HEARTBEAT_INTERVAL_MS,
                    SLSConfiguration.AM_HEARTBEAT_INTERVAL_MS_DEFAULT);
            int dispatchTime = random.nextInt(heartbeatInterval);
            mockAM
                .init(dispatchTime, dispatchTime + 1000000L * heartbeatInterval,
                    heartbeatInterval);
            runner.schedule(mockAM);
          } catch (IOException e) {
            e.printStackTrace();
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      });
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption("ha", false, "");
    options.addOption("rmClient1", true, "");
    options.addOption("rmClient2", true, "");
    options.addOption("rmSche1", true, "");
    options.addOption("rmSche2", true, "");

    Option rmAddrOpt = new Option("clientaddress", true, "");
    //    rmAddrOpt.setRequired(true);

    Option scheAddrOpt = new Option("scheaddress", true, "");
    //    scheAddrOpt.setRequired(true);

    Option appNumOpt = new Option("appnum", true, "");
    appNumOpt.setRequired(true);

    options.addOption(rmAddrOpt);
    options.addOption(appNumOpt);
    options.addOption(scheAddrOpt);

    options.addOption("poolsize", true, "");
    options.addOption("heartbeatinterval", true, "");

    options.addOption("labels", true, "");

    CommandLineParser parser = new GnuParser();
    CommandLine cmd = parser.parse(options, args);

    if (cmd.hasOption("ha")) {
      isHA = true;
    }
    if (isHA) {
      if (!cmd.hasOption("rmClient1") || !cmd.hasOption("rmClient2")) {
        throw new Exception(
            "Need rmClient1 & rmClient2 address when ha enabled.");
      }
      rmClient1 = cmd.getOptionValue("rmClient1");
      rmClient2 = cmd.getOptionValue("rmClient2");
      if (!cmd.hasOption("rmSche1") || !cmd.hasOption("rmSche2")) {
        throw new Exception("Need rmSche1 & rmSche2 address when ha enabled.");
      }
      rmSche1 = cmd.getOptionValue("rmSche1");
      rmSche2 = cmd.getOptionValue("rmSche2");
    } else {
      if (cmd.hasOption("scheaddress")) {
        rmScheAddress = cmd.getOptionValue("scheaddress");
      }
      if (cmd.hasOption("clientaddress")) {
        rmClientAddress = cmd.getOptionValue("clientaddress");
      }
    }

    if (cmd.hasOption("appnum")) {
      appNum = Integer.valueOf(cmd.getOptionValue("appnum"));
    }
    if (cmd.hasOption("poolsize")) {
      poolSize = Integer.valueOf(cmd.getOptionValue("poolsize"));
    } else {
      poolSize = 100;
    }
    if (cmd.hasOption("heartbeatinterval")) {
      heartbeatInterval =
          Integer.valueOf(cmd.getOptionValue("heartbeatinterval"));
    }

    if (cmd.hasOption("labels")) {
      String labelsInput = cmd.getOptionValue("labels");
      String[] split = labelsInput.split(":");
      if (split.length != 2) {
        throw new Exception("wrong labels input.");
      }
      String[] labels = split[0].split(",");
      String[] weights = split[1].split(",");
      if (labels.length != weights.length) {
        throw new Exception("label size should be same with weight.");
      }
      for (int i = 0; i < labels.length; i++ ) {
        labelSet.put(i, labels[i]);
      }
      LOG.info("label: " + labelSet);
      int sum = 0;
      for (int i = 0; i < weights.length; i++) {
        int weight = Integer.valueOf(weights[i]);
        sum += weight;
      }
      int tmp = 0;
      for (int i = 0; i < weights.length; i++) {
        int weight = Integer.valueOf(weights[i]);
        int index = (weight + tmp) * 100 / sum;
        tmp += weight;
        weightsList.add(index);
      }
      LOG.info("weight: " + weightsList);
    }

    start();
    return 0;
  }

  static class MockAM extends TaskRunner.Task {

    private ApplicationClientProtocol rmClient;
    private ApplicationMasterProtocol amsClient;
    private ApplicationId appId;
    private UserGroupInformation userUgi;
    private int id;
    private Configuration conf;
    private AllocateRequest allocateRequest =
        Records.newRecord(AllocateRequest.class);
    private int responseId = 0;

    private MockAMRunner runner;
    private Map<Integer, String> labels;
    private List<Integer> weights;

    public MockAM(Configuration conf, int id, MockAMRunner runner, Map<Integer, String> labels, List<Integer> weights)
        throws IOException, YarnException {
      this.runner = runner;
      this.conf = conf;
      this.id = id;
      this.rmClient =
          ClientRMProxy.createRMProxy(conf, ApplicationClientProtocol.class);
      GetNewApplicationRequest request = (GetNewApplicationRequest) Records
          .newRecord(GetNewApplicationRequest.class);
      this.appId = rmClient.getNewApplication(request).getApplicationId();
      ResourceRequest rr = Records.newRecord(ResourceRequest.class);
      rr.setCapability(Resource.newInstance(1024, 1));
      rr.setNumContainers(5);
      rr.setResourceName(ResourceRequest.ANY);
      rr.setPriority(Priority.newInstance(5));
      rr.setAllocationRequestId(0);
      rr.setExecutionTypeRequest(
          ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED));
      allocateRequest.getAskList().add(rr);
      this.labels = labels;
      this.weights = weights;
    }

    @Override
    public void firstStep() throws Exception {
      SubmitApplicationRequest submitRequest =
          Records.newRecord(SubmitApplicationRequest.class);

      ApplicationSubmissionContext context =
          Records.newRecord(ApplicationSubmissionContext.class);

      context.setApplicationId(appId);
      context.setApplicationName("app" + id);
      context.setQueue("default");

      if (labels.size() > 0) {
        int index = getIndex(random.nextInt(100));
        if (!labels.get(index).equals("default")) {
          context.setNodeLabelExpression(labels.get(index));
        }
      }

      ContainerLaunchContext amContainer =
          Records.newRecord(ContainerLaunchContext.class);
      Resource resource = BuilderUtils.newResource(1024, 1);
      context.setResource(resource);
      context.setAMContainerSpec(amContainer);
      submitRequest.setApplicationSubmissionContext(context);

      context.setUnmanagedAM(true);
      context.setKeepContainersAcrossApplicationAttempts(false);

      rmClient.submitApplication(submitRequest);
      monitorCurrentAppAttempt(appId, EnumSet
              .of(YarnApplicationState.ACCEPTED, YarnApplicationState.RUNNING,
                  YarnApplicationState.KILLED, YarnApplicationState.FAILED,
                  YarnApplicationState.FINISHED),
          YarnApplicationAttemptState.LAUNCHED);

      Token<AMRMTokenIdentifier> amrmToken = getAMRMToken();
      userUgi = UserGroupInformation.createRemoteUser("yarn");
      amsClient = AMRMClientUtils
          .createRMProxy(conf, ApplicationMasterProtocol.class, userUgi,
              amrmToken);
      registerApplicationMaster();
    }

    public int getIndex(int random) {
      for (int i = 0; i < weights.size(); i++) {
        if (random > weights.get(i)) {
          continue;
        } else {
          return i;
        }
      }
      return 0;
    }

    private Token<AMRMTokenIdentifier> getAMRMToken()
        throws IOException, YarnException {
      Token<AMRMTokenIdentifier> token = null;
      org.apache.hadoop.yarn.api.records.Token amrmToken =
          getApplicationReport(this.appId).getAMRMToken();
      if (amrmToken != null) {
        token = ConverterUtils.convertFromYarn(amrmToken, (Text) null);
      } else {
        LOG.warn(
            "AMRMToken not found in the application report for application: {}",
            this.appId);
      }
      return token;
    }

    private void registerApplicationMaster() throws IOException, YarnException {
      RegisterApplicationMasterRequest amRegisterRequest =
          Records.newRecord(RegisterApplicationMasterRequest.class);
      amRegisterRequest.setHost("localhost");
      amRegisterRequest.setRpcPort(1000);
      amRegisterRequest.setTrackingUrl("localhost:1000");
      amsClient.registerApplicationMaster(amRegisterRequest);
      responseId = 0;
      LOG.info("Register the application master for application {}", appId);
    }

    private ApplicationAttemptReport monitorCurrentAppAttempt(
        ApplicationId appId, Set<YarnApplicationState> appStates,
        YarnApplicationAttemptState attemptState)
        throws IOException, YarnException {

      ApplicationAttemptId appAttemptId = null;
      while (true) {
        if (appAttemptId == null) {
          ApplicationReport report = getApplicationReport(appId);
          YarnApplicationState state = report.getYarnApplicationState();
          if (appStates.contains(state)) {
            if (state != YarnApplicationState.ACCEPTED) {
              throw new YarnRuntimeException(
                  "Received non-accepted application state: " + state + " for "
                      + appId
                      + ". This is likely because this is not the first "
                      + "app attempt in home sub-cluster, and AMRMProxy HA "
                      + "(yarn.nodemanager.amrmproxy.ha.enable) is not enabled.");
            }
            appAttemptId =
                getApplicationReport(appId).getCurrentApplicationAttemptId();
          } else {
            LOG.info("Current application state of {} is {}, will retry later.",
                appId, state);
          }
        }

        if (appAttemptId != null) {
          GetApplicationAttemptReportRequest req =
              Records.newRecord(GetApplicationAttemptReportRequest.class);
          req.setApplicationAttemptId(appAttemptId);
          ApplicationAttemptReport attemptReport =
              this.rmClient.getApplicationAttemptReport(req)
                  .getApplicationAttemptReport();
          if (attemptState
              .equals(attemptReport.getYarnApplicationAttemptState())) {
            return attemptReport;
          }
          LOG.info("Current attempt state of " + appAttemptId + " is "
              + attemptReport.getYarnApplicationAttemptState()
              + ", waiting for current attempt to reach " + attemptState);
        }
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          LOG.warn("Interrupted while waiting for current attempt of " + appId
              + " to reach " + attemptState);
        }
      }
    }

    private ApplicationReport getApplicationReport(ApplicationId appId)
        throws YarnException, IOException {
      GetApplicationReportRequest request =
          Records.newRecord(GetApplicationReportRequest.class);
      request.setApplicationId(appId);
      return this.rmClient.getApplicationReport(request).getApplicationReport();
    }

    @Override
    public void middleStep() throws Exception {
      allocateRequest.setResponseId(responseId);
      AllocateResponse response = null;
      try {
        response = amsClient.allocate(allocateRequest);
        responseId = response.getResponseId();
        allocateRequest.getAskList().clear();

        for (NMToken token : response.getNMTokens()) {
          String nodeId = token.getNodeId().getHost() + ":45455";
          LOG.info("MockAM middleStep nodeId: " + nodeId);
          runner.nmTokenCache.setToken(nodeId, token.getToken());
        }
        if (response.getAllocatedContainers() != null && !response
            .getAllocatedContainers().isEmpty()) {
          for (Container c : response.getAllocatedContainers()) {
            LOG.info(
                "c_id: " + c.getId() + " node_id: " + c.getNodeId().getHost()
                    + " node_port: " + c.getNodeId().getPort());
            startContainer(c);
          }
        }

      } catch (ApplicationMasterNotRegisteredException e) {
        registerApplicationMaster();
      }
    }

    public void startContainer(Container container) {
      ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData
          proxy = null;
      try {
        proxy = runner.cmProxy
            .getProxy(container.getNodeId().getHost() + ":45455",
                container.getId());
        Map<String, ByteBuffer> serviceData = new HashMap<>();
        LOG.info(
            "startContainer nodeId: " + container.getNodeId().toString());
        byte[] data = container.getNodeId().toString().getBytes();
        ByteBuffer bb = ByteBuffer.allocate(data.length);
        bb.put(data);
        serviceData.put("ADDR", bb);
        ContainerLaunchContext containerLaunchContext = ContainerLaunchContext
            .newInstance(null, null, null, serviceData, null, null);
        StartContainerRequest scRequest = StartContainerRequest
            .newInstance(containerLaunchContext, container.getContainerToken());
        List<StartContainerRequest> list =
            new ArrayList<StartContainerRequest>();
        list.add(scRequest);
        StartContainersRequest allRequests =
            StartContainersRequest.newInstance(list);
        try {
          StartContainersResponse response =
              proxy.getContainerManagementProtocol()
                  .startContainers(allRequests);
        } catch (YarnException e) {
          e.printStackTrace();
        } catch (IOException e) {
          e.printStackTrace();
        }
      } catch (SecretManager.InvalidToken invalidToken) {
        invalidToken.printStackTrace();
      }
    }

    @Override
    public void lastStep() throws Exception {

    }
  }
}
