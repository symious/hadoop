package org.apache.hadoop.yarn.sls;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.ServerRMProxy;
import org.apache.hadoop.yarn.server.api.protocolrecords.*;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.sls.conf.SLSConfiguration;
import org.apache.hadoop.yarn.sls.nodemanager.NodeInfo;
import org.apache.hadoop.yarn.sls.scheduler.TaskRunner;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MockNMRunner extends Configured
    implements Tool, ContainerManagementProtocol {
  private static TaskRunner runner = new TaskRunner();
  private String rmAddress;
  private int nmNum;
  private int poolSize;
  private int heartbeatInterval;
  private boolean isHA;
  private String rm1;
  private String rm2;
  private Server server;
  private InetSocketAddress connectAddress;

  private Map<Integer, String> labelSet = new HashMap<>();
  private List<Integer> weightsList = new ArrayList<>();
  private static Random random = new Random();

  private Map<String, MockNM> mockNMMap = new ConcurrentHashMap<>();

  public final static Logger LOG = LoggerFactory.getLogger(MockNMRunner.class);

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new YarnConfiguration(), new MockNMRunner(), args);
  }

  public void startContainerServer() {
    YarnRPC rpc = YarnRPC.create(getConf());
    final InetSocketAddress initialAddress = getConf()
        .getSocketAddr(YarnConfiguration.NM_BIND_HOST,
            YarnConfiguration.NM_ADDRESS, YarnConfiguration.DEFAULT_NM_ADDRESS,
            YarnConfiguration.DEFAULT_NM_PORT);
    Configuration serverConf = new Configuration(getConf());
    server =
        rpc.getServer(ContainerManagementProtocol.class, this, initialAddress,
            serverConf, null, getConf()
                .getInt(YarnConfiguration.NM_CONTAINER_MGR_THREAD_COUNT,
                    YarnConfiguration.DEFAULT_NM_CONTAINER_MGR_THREAD_COUNT));
    server.start();
    connectAddress = NetUtils.getConnectAddress(server);
  }

  public void start() {
    if (isHA) {
      getConf().setBoolean("yarn.resourcemanager.ha.enabled", true);
      getConf().set("yarn.resourcemanager.ha.rm-ids", "rm1,rm2");
      getConf().set("yarn.resourcemanager.resource-tracker.address.rm1", rm1);
      getConf().set("yarn.resourcemanager.resource-tracker.address.rm2", rm2);
    } else {
      getConf().set("yarn.resourcemanager.resource-tracker.address", rmAddress);
      getConf().setBoolean("yarn.resourcemanager.ha.enabled", false);
    }
    getConf().setLong("yarn.resourcemanager.connect.retry-interval.ms", 15000L);

    if (heartbeatInterval > 0) {
      getConf()
          .setInt(SLSConfiguration.NM_HEARTBEAT_INTERVAL_MS, heartbeatInterval);
    }

    runner.setQueueSize(poolSize);
    runner.start();
    Random random = new Random();
    ExecutorService executorService = Executors.newFixedThreadPool(100);
    for (int i = 0; i < nmNum; i++) {
      int finalI = i;
      executorService.submit(new Runnable() {
        @Override
        public void run() {
          try {
            MockNM mockNM =
                new MockNM(getConf(), finalI, connectAddress, labelSet,
                    weightsList);
            mockNMMap.put(mockNM.node.getNodeID().toString(), mockNM);
            LOG.info("Run node: " + mockNM.node.getNodeID());
            int heartbeatInterval = getConf()
                .getInt(SLSConfiguration.NM_HEARTBEAT_INTERVAL_MS,
                    SLSConfiguration.NM_HEARTBEAT_INTERVAL_MS_DEFAULT);
            int dispatchTime = random.nextInt(heartbeatInterval);
            mockNM
                .init(dispatchTime, dispatchTime + 1000000L * heartbeatInterval,
                    heartbeatInterval);
            runner.schedule(mockNM);
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

    Option rmAddrOpt = new Option("rm", true, "");
    Option nmNumOpt = new Option("nmnum", true, "");

    nmNumOpt.setRequired(true);
    options.addOption(rmAddrOpt);
    options.addOption(nmNumOpt);

    options.addOption("ha", false, "");
    options.addOption("rm1", true, "");
    options.addOption("rm2", true, "");

    options.addOption("poolsize", true, "");
    options.addOption("heartbeatinterval", true, "");
    options.addOption("labels", true, "");

    CommandLineParser parser = new GnuParser();
    CommandLine cmd = parser.parse(options, args);

    if (cmd.hasOption("ha")) {
      isHA = true;
    }
    if (isHA) {
      if (!cmd.hasOption("rm1") || !cmd.hasOption("rm2")) {
        throw new Exception("Need rm1 & rm2 address when ha enabled.");
      }
      rm1 = cmd.getOptionValue("rm1");
      rm2 = cmd.getOptionValue("rm2");
    } else {
      if (cmd.hasOption("rm")) {
        rmAddress = cmd.getOptionValue("rm");
      }
    }

    if (cmd.hasOption("nmnum")) {
      nmNum = Integer.valueOf(cmd.getOptionValue("nmnum"));
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
      for (int i = 0; i < labels.length; i++) {
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
    startContainerServer();
    start();
    return 0;
  }

  static class MockNM extends TaskRunner.Task {
    private ResourceTracker resourceTracker;
    private RMNode node;
    private MasterKey masterKey;
    private MasterKey containerTokenKey;
    private int responseId = 0;
    private UserGroupInformation userUgi;

    private Map<Integer, String> labels;
    private List<Integer> weights;

    private RegisterNodeManagerRequest req;

    private Map<ContainerId, ContainerTokenIdentifier> containers =
        new ConcurrentHashMap<>();

    public MockNM(Configuration conf, int nodeId, InetSocketAddress realAddress,
        Map<Integer, String> labels, List<Integer> weights)
        throws IOException, InterruptedException {
      Resource resource = Resource.newInstance(20480, 32);
      this.node = NodeInfo
          .newNodeInfo("", InetAddress.getLocalHost().getHostName(), resource);
      userUgi = UserGroupInformation.createRemoteUser("yarn");

      this.resourceTracker =
          userUgi.doAs(new PrivilegedExceptionAction<ResourceTracker>() {
            @Override
            public ResourceTracker run() throws Exception {
              return ServerRMProxy.createRMProxy(conf, ResourceTracker.class);
            }
          });
      this.labels = labels;
      this.weights = weights;
    }

    @Override
    public void firstStep() throws Exception {
      this.req = Records.newRecord(RegisterNodeManagerRequest.class);
      req.setNodeId(node.getNodeID());
      req.setResource(node.getTotalCapability());
      req.setHttpPort(80);

      if (labels.size() > 0) {
        Set<NodeLabel> set = new HashSet<>();
        int index = getIndex(random.nextInt(100));
        if (labels.get(index).equals("default")) {
          set.add(NodeLabel.newInstance(""));
        } else {
          set.add(NodeLabel.newInstance(labels.get(index)));
        }
        LOG.info("index: " + index);
        req.setNodeLabels(set);
      }

      List<NMContainerStatus> containerReports = getNMContainerStatuses();
      req.setContainerStatuses(containerReports);

      RegisterNodeManagerResponse response =
          resourceTracker.registerNodeManager(req);
      masterKey = response.getNMTokenMasterKey();
      containerTokenKey = response.getContainerTokenMasterKey();
      if (response.getNodeAction() == NodeAction.SHUTDOWN) {
        LOG.info("register NM shutdown");
      }
      LOG.info(node.getNodeID() + " registered.");
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

    public List<NMContainerStatus> getNMContainerStatuses() {
      List<NMContainerStatus> containerStatuses =
          new ArrayList<NMContainerStatus>();
      for (ContainerTokenIdentifier container : containers.values()) {
        NMContainerStatus status = NMContainerStatus
            .newInstance(container.getContainerID(), container.getVersion(),
                ContainerState.RUNNING, container.getResource(), "",
                ContainerExitStatus.INVALID, container.getPriority(),
                container.getCreationTime(), container.getNodeLabelExpression(),
                container.getExecutionType(),
                container.getAllocationRequestId());
        status.setAllocationTags(container.getAllcationTags());
        containerStatuses.add(status);
      }
      return containerStatuses;
    }

    public List<ContainerStatus> getContainerStatuses() {
      List<ContainerStatus> containerStatuses =
          new ArrayList<ContainerStatus>();
      for (ContainerTokenIdentifier container : containers.values()) {
        ContainerStatus containerStatus = ContainerStatus
            .newInstance(container.getContainerID(), ContainerState.RUNNING, "",
                ContainerExitStatus.INVALID);
        containerStatuses.add(containerStatus);
      }
      return containerStatuses;
    }

    @Override
    public void middleStep() throws Exception {
      NodeHeartbeatRequest beatRequest =
          Records.newRecord(NodeHeartbeatRequest.class);
      beatRequest.setLastKnownNMTokenMasterKey(masterKey);
      beatRequest.setLastKnownContainerTokenMasterKey(containerTokenKey);
      NodeStatus ns = Records.newRecord(NodeStatus.class);
      ns.setNodeId(node.getNodeID());
      ns.setResponseId(responseId);
      ns.setNodeHealthStatus(NodeHealthStatus.newInstance(true, "", 0));

      ResourceUtilization resourceUtilization =
          ResourceUtilization.newInstance(0, 0, 0);
      ns.setContainersUtilization(resourceUtilization);
      ns.setNodeUtilization(resourceUtilization);

      ns.setContainersStatuses(getContainerStatuses());

      beatRequest.setNodeStatus(ns);
      NodeHeartbeatResponse beatResponse =
          resourceTracker.nodeHeartbeat(beatRequest);
      if (beatResponse.getNodeAction() == NodeAction.NORMAL) {
          responseId = beatResponse.getResponseId();
          setRepeatInterval(beatResponse.getNextHeartBeatInterval());
      }

      if (beatResponse.getNodeAction() == NodeAction.SHUTDOWN) {
        LOG.info("hearbeat NM shutdown");
      }

      if (beatResponse.getNodeAction() == NodeAction.RESYNC) {
        LOG.info("hearbeat NM resync " + node.getNodeID());
        responseId = 0;
        LOG.info("reregister start " + node.getNodeID());
        List<NMContainerStatus> containerReports = getNMContainerStatuses();
        req.getNMContainerStatuses().clear();
        req.setContainerStatuses(containerReports);
        RegisterNodeManagerResponse response =
            resourceTracker.registerNodeManager(req);
        masterKey = response.getNMTokenMasterKey();
        containerTokenKey = response.getContainerTokenMasterKey();
        if (response.getNodeAction() == NodeAction.SHUTDOWN) {
          LOG.info("register NM shutdown");
        }
        LOG.info("reregister end " + node.getNodeID());
      }
    }

    @Override
    public void lastStep() throws Exception {

    }
  }

  @Override
  public StartContainersResponse startContainers(
      StartContainersRequest startContainersRequest)
      throws YarnException, IOException {
    for (StartContainerRequest request : startContainersRequest
        .getStartContainerRequests()) {
      ByteBuffer bb =
          request.getContainerLaunchContext().getServiceData().get("ADDR");
      byte[] data = new byte[bb.capacity()];
      bb.get(data);

      ContainerTokenIdentifier containerTokenIdentifier =
          BuilderUtils.newContainerTokenIdentifier(request.getContainerToken());
      ContainerId containerId = containerTokenIdentifier.getContainerID();

      String addr = new String(data);
      LOG.info("startContainers get addr: " + addr);
      if (mockNMMap.containsKey(addr)) {
        LOG.info("startContainers containerId: " + containerId);
        //        mockNMMap.get(addr).runningContainer.add(containerId);
        mockNMMap.get(addr).containers
            .put(containerId, containerTokenIdentifier);
      }
    }
    return StartContainersResponse.newInstance(null, null, null);
  }

  @Override
  public StopContainersResponse stopContainers(
      StopContainersRequest stopContainersRequest)
      throws YarnException, IOException {
    return null;
  }

  @Override
  public GetContainerStatusesResponse getContainerStatuses(
      GetContainerStatusesRequest getContainerStatusesRequest)
      throws YarnException, IOException {
    return null;
  }

  @Override
  public IncreaseContainersResourceResponse increaseContainersResource(
      IncreaseContainersResourceRequest increaseContainersResourceRequest)
      throws YarnException, IOException {
    return null;
  }

  @Override
  public ContainerUpdateResponse updateContainer(
      ContainerUpdateRequest containerUpdateRequest)
      throws YarnException, IOException {
    return null;
  }

  @Override
  public SignalContainerResponse signalToContainer(
      SignalContainerRequest signalContainerRequest)
      throws YarnException, IOException {
    return null;
  }

  @Override
  public ResourceLocalizationResponse localize(
      ResourceLocalizationRequest resourceLocalizationRequest)
      throws YarnException, IOException {
    return null;
  }

  @Override
  public ReInitializeContainerResponse reInitializeContainer(
      ReInitializeContainerRequest reInitializeContainerRequest)
      throws YarnException, IOException {
    return null;
  }

  @Override
  public RestartContainerResponse restartContainer(ContainerId containerId)
      throws YarnException, IOException {
    return null;
  }

  @Override
  public RollbackResponse rollbackLastReInitialization(ContainerId containerId)
      throws YarnException, IOException {
    return null;
  }

  @Override
  public CommitResponse commitLastReInitialization(ContainerId containerId)
      throws YarnException, IOException {
    return null;
  }

  @Override
  public GetLocalizationStatusesResponse getLocalizationStatuses(
      GetLocalizationStatusesRequest getLocalizationStatusesRequest)
      throws YarnException, IOException {
    return null;
  }
}
