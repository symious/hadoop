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
package org.apache.hadoop.yarn.sls;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.rumen.JobTraceReader;
import org.apache.hadoop.tools.rumen.LoggedJob;
import org.apache.hadoop.tools.rumen.LoggedTask;
import org.apache.hadoop.tools.rumen.LoggedTaskAttempt;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.sls.appmaster.AMSimulator;
import org.apache.hadoop.yarn.sls.appmaster.AMSimulatorForRealRM;
import org.apache.hadoop.yarn.sls.conf.SLSConfiguration;
import org.apache.hadoop.yarn.sls.nodemanager.NMSimulatorForRealRM;
import org.apache.hadoop.yarn.sls.scheduler.ContainerSimulator;
import org.apache.hadoop.yarn.sls.scheduler.TaskRunner;
import org.apache.hadoop.yarn.sls.utils.SLSUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;

@Private
@Unstable
public class SLSRunnerForRealRM {
  // RM, Runner
  private static TaskRunner amRunner = new TaskRunner();
  private static TaskRunner nmRunner = new TaskRunner();
  private String[] inputTraces;
  private Configuration conf;
  private Configuration yarnConf;
  private Map<String, Integer> queueAppNumMap;

  // NM simulator
  private HashMap<NodeId, NMSimulatorForRealRM> nmMap;
  private int nmMemoryMB, nmVCores;
  private String nodeFile;

  // AM simulator
  private int AM_ID;
  private Map<String, AMSimulatorForRealRM> amMap;
  private Set<String> trackedApps;
  private Map<String, Class> amClassMap;
  private static  AtomicInteger remainingApps = new AtomicInteger(0);

  // metrics
  private String metricsOutputDir;
  private boolean printSimulation;

  // other simulation information
  private int numNMs, numRacks, numAMs, numTasks;
  private long maxRuntime;
  public final static Map<String, Object> simulateInfoMap =
      new HashMap<String, Object>();

  public BlockingQueue<AMSimulatorForRealRM> noRunningAM = new LinkedBlockingQueue<AMSimulatorForRealRM>();

  private YarnClient rmClient;

  // logger
  public final static Logger LOG = Logger.getLogger(SLSRunner.class);

  // input traces, input-rumen or input-sls
  private boolean isSLS;

  private int submitAppInteralMs = 1000 * 60;
  private int submitAppInteralCount = 250;
  private int parallAppMaxCount = 1000;

  public SLSRunnerForRealRM(boolean isSLS, String inputTraces[], String nodeFile,
      String outputDir, Set<String> trackedApps,
      boolean printsimulation)
      throws IOException, ClassNotFoundException {
    this.isSLS = isSLS;
    this.inputTraces = inputTraces.clone();
    this.nodeFile = nodeFile;
    this.trackedApps = trackedApps;
    this.printSimulation = printsimulation;
    metricsOutputDir = outputDir;

    nmMap = new HashMap<NodeId, NMSimulatorForRealRM>();
    queueAppNumMap = new HashMap<String, Integer>();
    amMap = new HashMap<String, AMSimulatorForRealRM>();
    amClassMap = new HashMap<String, Class>();

    // runner configuration
    conf = new Configuration(false);
    conf.addResource("sls-runner.xml");
    yarnConf = new YarnConfiguration();

    //init YANRClient
    initYARNClient();

    // runner
    int amPoolSize = conf.getInt(SLSConfiguration.RUNNER_AM_POOL_SIZE,
        SLSConfiguration.RUNNER_AM_POOL_SIZE_DEFAULT);
    int nmPoolSize = conf.getInt(SLSConfiguration.RUNNER_NM_POOL_SIZE,SLSConfiguration.RUNNER_NM_POOL_SIZE_DEFAULT);

    submitAppInteralMs = conf.getInt(SLSConfiguration.AM_SUBMIT_INTERVAL_MS,
        SLSConfiguration.AM_SUBMIT_INTERVAL_MS_DEFAULT);
    submitAppInteralCount = conf.getInt(SLSConfiguration.AM_SUBMIT_INTERVAL_COUNT,
        SLSConfiguration.AM_SUBMIT_INTERVAL_COUNT_DEFAULT);
    parallAppMaxCount = conf.getInt(SLSConfiguration.AM_MAX_PARALLEL_NUM,1000);

    LOG.info("submitAppInteralMs:" + submitAppInteralMs);
    LOG.info("submitAppInteralCount:" + submitAppInteralCount);
    LOG.info("parallAppMaxCount:" + parallAppMaxCount);

    LOG.info("amPoolSize:" + amPoolSize);
    LOG.info("nmPoolSize:" + nmPoolSize);
    SLSRunnerForRealRM.amRunner.setQueueSize(amPoolSize);
    SLSRunnerForRealRM.nmRunner.setQueueSize(nmPoolSize);
    // <AMType, Class> map
    for (Map.Entry e : conf) {
      String key = e.getKey().toString();
      if (key.startsWith(SLSConfiguration.AM_TYPE)) {
        String amType = key.substring(SLSConfiguration.AM_TYPE.length() + 1);
        amClassMap.put(amType, Class.forName(conf.get(key)));
      }
    }
  }

  private void initYARNClient()
  {
    YarnConfiguration yarnConf = new YarnConfiguration(this.yarnConf);
    rmClient = YarnClient.createYarnClient();
    rmClient.init(yarnConf);
    rmClient.start();
  }

  public void start() throws Exception {
    // start node managers
    startNM();
    nmRunner.start();
    // start application masters
    startAM();
    // print out simulation info
    printSimulationInfo();
    // blocked until all nodes RUNNING
    // starting the runner once everything is ready to go,
    waitForNodesRunning();
    amRunner.start();
    continuousSubmitApp();
  }

  private void continuousSubmitApp() {
    Thread submitAppThread = new Thread(new Runnable() {
      @Override
      public void run() {
        LOG.info("running continuousSubmitApp thread,current no Running am size:" + noRunningAM.size());
        Set<String> appTypes = new HashSet<String>();
        appTypes.add("YARN");
        int amPoolSize = conf.getInt(SLSConfiguration.RUNNER_AM_POOL_SIZE,
            SLSConfiguration.RUNNER_AM_POOL_SIZE_DEFAULT);
        try {
          while (noRunningAM.size() > 0)
          {
            Thread.sleep(submitAppInteralMs);
            LOG.info("ready continuous submit:" + submitAppInteralCount + " apps");
            int count = 0;
            int currentParallAppCount = 0;
            long startTimeForGetApplications = System.currentTimeMillis();
            List<ApplicationReport> report =
                rmClient.getApplications();

            for(ApplicationReport app:report){
              if(app.getFinalApplicationStatus().equals(FinalApplicationStatus.UNDEFINED)){
                currentParallAppCount ++;
              }
            }
            long getParallAppsCostTime = System.currentTimeMillis() - startTimeForGetApplications;
            while ( count < submitAppInteralCount
                && currentParallAppCount < parallAppMaxCount && SLSRunnerForRealRM.getAMRunner().getQueueSize() < parallAppMaxCount - amPoolSize) {
              AMSimulatorForRealRM am = (AMSimulatorForRealRM) noRunningAM.poll();
              if (am != null) {
                LOG.info("continuousSubmitApp sls has noRunning am size:"
                    + noRunningAM.size() + ",schedule am_id:" + am.getId()
                    + ",current_thread:" + Thread.currentThread());
                SLSRunnerForRealRM.getAMRunner().schedule(am);
              }
              count++;
            }
            LOG.info("continuous submited:" + count + " apps,currentParallAppCount:" + currentParallAppCount
                + ",getParallAppsCostTime(ms):" + getParallAppsCostTime);
          }
        } catch (Exception ex) {
          LOG.error(ex);
        }
      }
    });
    submitAppThread.setDaemon(true);
    submitAppThread.start();
  }
  private void startNM() throws YarnException, IOException, InterruptedException {
    // nm configuration
    LOG.info("startNM begin ---------");
    nmMemoryMB = conf.getInt(SLSConfiguration.NM_MEMORY_MB,
        SLSConfiguration.NM_MEMORY_MB_DEFAULT);
    nmVCores = conf.getInt(SLSConfiguration.NM_VCORES,
        SLSConfiguration.NM_VCORES_DEFAULT);
    int heartbeatInterval = conf.getInt(
        SLSConfiguration.NM_HEARTBEAT_INTERVAL_MS,
        SLSConfiguration.NM_HEARTBEAT_INTERVAL_MS_DEFAULT);
    // nm information (fetch from topology file, or from sls/rumen json file)
    Set<SLSRunner.NodeDetails> nodeSet = new HashSet<>();
    if (nodeFile.isEmpty()) {
      if (isSLS) {
        for (String inputTrace : inputTraces) {
          nodeSet.addAll(SLSUtils.parseNodesFromSLSTrace(inputTrace));
        }
      } else {
        for (String inputTrace : inputTraces) {
          nodeSet.addAll(SLSUtils.parseNodesFromRumenTrace(inputTrace));
        }
      }

    } else {
      nodeSet.addAll(SLSUtils.parseNodesFromNodeFile(nodeFile, Resources
          .createResource(nmMemoryMB, nmVCores)));
    }
    // create NM simulators
    Random random = new Random();
    Set<String> rackSet = new HashSet<String>();
    for (SLSRunner.NodeDetails nodeDetails : nodeSet) {
      String hostName = nodeDetails.getHostname();
      // we randomize the heartbeat start time from zero to 1 interval
      NMSimulatorForRealRM nm = new NMSimulatorForRealRM();
      nm.init(hostName, nmMemoryMB, nmVCores,
          random.nextInt(heartbeatInterval), heartbeatInterval, yarnConf);
      nmMap.put(nm.getNode().getNodeID(), nm);
      rackSet.add(nm.getNode().getRackName());
      nmRunner.schedule(nm);
    }
    numRacks = rackSet.size();
    numNMs = nmMap.size();
    LOG.info("startNM end ---------");
  }

  private void waitForNodesRunning() throws InterruptedException, YarnException, IOException {
    long startTimeMS = System.currentTimeMillis();
    while (true) {
      List<NodeReport> nodes = rmClient.getNodeReports(NodeState.RUNNING);
      int numRunningNodes = nodes==null ? 0:nodes.size();
      if (numRunningNodes >= numNMs) {
        break;
      }
      LOG.info(MessageFormat.format("SLSRunner is waiting for all " +
              "nodes RUNNING. {0} of {1} NMs initialized.",
          numRunningNodes, numNMs));
      Thread.sleep(1000);
    }
    LOG.info(MessageFormat.format("SLSRunner takes {0} ms to launch all nodes.",
        (System.currentTimeMillis() - startTimeMS)));
  }

  @SuppressWarnings("unchecked")
  private void startAM() throws YarnException, IOException {
    // application/container configuration
    LOG.info("startAM begin --------------");
    int heartbeatInterval = conf.getInt(
        SLSConfiguration.AM_HEARTBEAT_INTERVAL_MS,
        SLSConfiguration.AM_HEARTBEAT_INTERVAL_MS_DEFAULT);

    int parallelInitNum = this.conf.getInt(SLSConfiguration.AM_INIT_PARALLEL_NUM, 1000);

    int containerMemoryMB = conf.getInt(SLSConfiguration.CONTAINER_MEMORY_MB,
        SLSConfiguration.CONTAINER_MEMORY_MB_DEFAULT);
    int containerVCores = conf.getInt(SLSConfiguration.CONTAINER_VCORES,
        SLSConfiguration.CONTAINER_VCORES_DEFAULT);
    Resource containerResource =
        BuilderUtils.newResource(containerMemoryMB, containerVCores);

    // application workload
    if (isSLS) {
      startAMFromSLSTraces(containerResource, heartbeatInterval,parallelInitNum);
    } else {
      startAMFromRumenTraces(containerResource, heartbeatInterval);
    }
    numAMs = amMap.size();
    remainingApps.set(numAMs);
    LOG.info("startAM end --------------");
  }

  /**
   * parse workload information from sls trace files
   */
  @SuppressWarnings("unchecked")
  private void startAMFromSLSTraces(Resource containerResource,
      int heartbeatInterval,
      int parallelInitNum) throws IOException {
    // parse from sls traces
    JsonFactory jsonF = new JsonFactory();
    ObjectMapper mapper = new ObjectMapper();
    for (String inputTrace : inputTraces) {
      Reader input = new FileReader(inputTrace);
      try {
        Iterator<Map> i = mapper.readValues(jsonF.createJsonParser(input),
            Map.class);
        while (i.hasNext()) {
          Map jsonJob = i.next();

          // load job information
          long jobStartTime = Long.parseLong(
              jsonJob.get("job.start.ms").toString());
          long jobFinishTime = Long.parseLong(
              jsonJob.get("job.end.ms").toString());

          String user = (String) jsonJob.get("job.user");
          if (user == null)  user = "default";
          String queue = jsonJob.get("job.queue.name").toString();

          String oldAppId = jsonJob.get("job.id").toString();
          boolean isTracked = trackedApps.contains(oldAppId);
          int queueSize = queueAppNumMap.containsKey(queue) ?
              queueAppNumMap.get(queue) : 0;
          queueSize ++;
          queueAppNumMap.put(queue, queueSize);
          // tasks
          List tasks = (List) jsonJob.get("job.tasks");
          if (tasks == null || tasks.size() == 0) {
            continue;
          }
          List<ContainerSimulator> containerList =
              new ArrayList<ContainerSimulator>();
          for (Object o : tasks) {
            Map jsonTask = (Map) o;
            String hostname = jsonTask.get("container.host").toString();
            long taskStart = Long.parseLong(
                jsonTask.get("container.start.ms").toString());
            long taskFinish = Long.parseLong(
                jsonTask.get("container.end.ms").toString());
            long lifeTime = taskFinish - taskStart;
            int priority = Integer.parseInt(
                jsonTask.get("container.priority").toString());
            String type = jsonTask.get("container.type").toString();
            containerList.add(new ContainerSimulator(containerResource,
                lifeTime, hostname, priority, type));
          }

          // create a new AM
          String amType = jsonJob.get("am.type").toString();
          AMSimulatorForRealRM amSim = (AMSimulatorForRealRM) ReflectionUtils.newInstance(
              amClassMap.get(amType), new Configuration());
          if (amSim != null) {
            amSim.init(AM_ID++, heartbeatInterval, containerList, yarnConf,
                this, jobStartTime, jobFinishTime, user, queue,
                isTracked, oldAppId);
            if (this.AM_ID <= parallelInitNum)
              amRunner.schedule(amSim);
            else
              this.noRunningAM.add(amSim);
            maxRuntime = Math.max(maxRuntime, jobFinishTime);
            numTasks += containerList.size();
            amMap.put(oldAppId, amSim);
          }
        }
      } finally {
        input.close();
      }
    }
  }

  /**
   * parse workload information from rumen trace files
   */
  @SuppressWarnings("unchecked")
  private void startAMFromRumenTraces(Resource containerResource,
      int heartbeatInterval)
      throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "file:///");
    long baselineTimeMS = 0;
    for (String inputTrace : inputTraces) {
      File fin = new File(inputTrace);
      JobTraceReader reader = new JobTraceReader(
          new Path(fin.getAbsolutePath()), conf);
      try {
        LoggedJob job = null;
        while ((job = reader.getNext()) != null) {
          // only support MapReduce currently
          String jobType = "mapreduce";
          String user = job.getUser() == null ?
              "default" : job.getUser().getValue();
          String jobQueue = job.getQueue().getValue();
          String oldJobId = job.getJobID().toString();
          long jobStartTimeMS = job.getSubmitTime();
          long jobFinishTimeMS = job.getFinishTime();
          if (baselineTimeMS == 0) {
            baselineTimeMS = jobStartTimeMS;
          }
          jobStartTimeMS -= baselineTimeMS;
          jobFinishTimeMS -= baselineTimeMS;
          if (jobStartTimeMS < 0) {
            LOG.warn("Warning: reset job " + oldJobId + " start time to 0.");
            jobFinishTimeMS = jobFinishTimeMS - jobStartTimeMS;
            jobStartTimeMS = 0;
          }

          boolean isTracked = trackedApps.contains(oldJobId);
          int queueSize = queueAppNumMap.containsKey(jobQueue) ?
              queueAppNumMap.get(jobQueue) : 0;
          queueSize ++;
          queueAppNumMap.put(jobQueue, queueSize);

          List<ContainerSimulator> containerList =
              new ArrayList<ContainerSimulator>();
          // map tasks
          for(LoggedTask mapTask : job.getMapTasks()) {
            LoggedTaskAttempt taskAttempt = mapTask.getAttempts()
                .get(mapTask.getAttempts().size() - 1);
            String hostname = taskAttempt.getHostName().getValue();
            long containerLifeTime = taskAttempt.getFinishTime()
                - taskAttempt.getStartTime();
            containerList.add(new ContainerSimulator(containerResource,
                containerLifeTime, hostname, 10, "map"));
          }

          // reduce tasks
          for(LoggedTask reduceTask : job.getReduceTasks()) {
            LoggedTaskAttempt taskAttempt = reduceTask.getAttempts()
                .get(reduceTask.getAttempts().size() - 1);
            String hostname = taskAttempt.getHostName().getValue();
            long containerLifeTime = taskAttempt.getFinishTime()
                - taskAttempt.getStartTime();
            containerList.add(new ContainerSimulator(containerResource,
                containerLifeTime, hostname, 20, "reduce"));
          }

          // create a new AM
          AMSimulatorForRealRM amSim = (AMSimulatorForRealRM) ReflectionUtils.newInstance(
              amClassMap.get(jobType), conf);
          if (amSim != null) {
            amSim.init(AM_ID ++, heartbeatInterval, containerList,
                null, this, jobStartTimeMS, jobFinishTimeMS, user, jobQueue,
                isTracked, oldJobId);
            amRunner.schedule(amSim);
            maxRuntime = Math.max(maxRuntime, jobFinishTimeMS);
            numTasks += containerList.size();
            amMap.put(oldJobId, amSim);
          }
        }
      } finally {
        reader.close();
      }
    }
  }

  private void printSimulationInfo() {
    if (printSimulation) {
      // node
      LOG.info("------------------------------------");
      LOG.info(MessageFormat.format("# nodes = {0}, # racks = {1}, capacity " +
              "of each node {2} MB memory and {3} vcores.",
          numNMs, numRacks, nmMemoryMB, nmVCores));
      LOG.info("------------------------------------");
      // job
      LOG.info(MessageFormat.format("# applications = {0}, # total " +
              "tasks = {1}, average # tasks per application = {2}",
          numAMs, numTasks, (int)(Math.ceil((numTasks + 0.0) / numAMs))));
      LOG.info("JobId\tQueue\tAMType\tDuration\t#Tasks");
      for (Map.Entry<String, AMSimulatorForRealRM> entry : amMap.entrySet()) {
        AMSimulatorForRealRM am = entry.getValue();
        LOG.info(entry.getKey() + "\t" + am.getQueue() + "\t" + am.getAMType()
            + "\t" + am.getDuration() + "\t" + am.getNumTasks());
      }
      LOG.info("------------------------------------");
      // queue
      LOG.info(MessageFormat.format("number of queues = {0}  average " +
              "number of apps = {1}", queueAppNumMap.size(),
          (int)(Math.ceil((numAMs + 0.0) / queueAppNumMap.size()))));
      LOG.info("------------------------------------");
      // runtime
      LOG.info(MessageFormat.format("estimated simulation time is {0}" +
          " seconds", (long)(Math.ceil(maxRuntime / 1000.0))));
      LOG.info("------------------------------------");
    }
    // package these information in the simulateInfoMap used  by other places
    simulateInfoMap.put("Number of racks", numRacks);
    simulateInfoMap.put("Number of nodes", numNMs);
    simulateInfoMap.put("Node memory (MB)", nmMemoryMB);
    simulateInfoMap.put("Node VCores", nmVCores);
    simulateInfoMap.put("Number of applications", numAMs);
    simulateInfoMap.put("Number of tasks", numTasks);
    simulateInfoMap.put("Average tasks per applicaion",
        (int)(Math.ceil((numTasks + 0.0) / numAMs)));
    simulateInfoMap.put("Number of queues", queueAppNumMap.size());
    simulateInfoMap.put("Average applications per queue",
        (int)(Math.ceil((numAMs + 0.0) / queueAppNumMap.size())));
    simulateInfoMap.put("Estimated simulate time (s)",
        (long)(Math.ceil(maxRuntime / 1000.0)));
  }

  public HashMap<NodeId, NMSimulatorForRealRM> getNmMap() {
    return nmMap;
  }

  public static TaskRunner getAMRunner() {
    return amRunner;
  }

  public static void decreaseRemainingApps() {
    remainingApps.decrementAndGet();
    if (remainingApps.get() <= 0) {
      LOG.info("All apps finished,so SLSRunner tears down.");
      System.exit(0);
    }
  }

  public static void main(String args[]) throws Exception {
    Options options = new Options();
    options.addOption("inputrumen", true, "input rumen files");
    options.addOption("inputsls", true, "input sls files");
    options.addOption("nodes", true, "input topology");
    options.addOption("output", true, "output directory");
    options.addOption("trackjobs", true,
        "jobs to be tracked during simulating");
    options.addOption("printsimulation", false,
        "print out simulation information");

    CommandLineParser parser = new GnuParser();
    CommandLine cmd = parser.parse(options, args);

    String inputRumen = cmd.getOptionValue("inputrumen");
    String inputSLS = cmd.getOptionValue("inputsls");
    String output = cmd.getOptionValue("output");

    if ((inputRumen == null && inputSLS == null) || output == null) {
      System.err.println();
      System.err.println("ERROR: Missing input or output file");
      System.err.println();
      System.err.println("Options: -inputrumen|-inputsls FILE,FILE... " +
          "-output FILE [-nodes FILE] [-trackjobs JobId,JobId...] " +
          "[-printsimulation]");
      System.err.println();
      System.exit(1);
    }

    File outputFile = new File(output);
    if (! outputFile.exists()
        && ! outputFile.mkdirs()) {
      System.err.println("ERROR: Cannot create output directory "
          + outputFile.getAbsolutePath());
      System.exit(1);
    }

    Set<String> trackedJobSet = new HashSet<String>();
    if (cmd.hasOption("trackjobs")) {
      String trackjobs = cmd.getOptionValue("trackjobs");
      String jobIds[] = trackjobs.split(",");
      trackedJobSet.addAll(Arrays.asList(jobIds));
    }

    String nodeFile = cmd.hasOption("nodes") ? cmd.getOptionValue("nodes") : "";

    boolean isSLS = inputSLS != null;
    String inputFiles[] = isSLS ? inputSLS.split(",") : inputRumen.split(",");
    SLSRunnerForRealRM sls = new SLSRunnerForRealRM(isSLS, inputFiles, nodeFile, output,
        trackedJobSet, cmd.hasOption("printsimulation"));
    sls.start();
  }
}
