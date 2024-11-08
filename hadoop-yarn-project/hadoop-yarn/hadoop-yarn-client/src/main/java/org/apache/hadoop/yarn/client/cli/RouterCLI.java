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
package org.apache.hadoop.yarn.client.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.ha.HAAdmin.UsageInfo;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeleteFederationApplicationRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeleteFederationApplicationResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;

public class RouterCLI extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(RouterCLI.class);

  // Command Constant
  private static final String CMD_EMPTY = "";
  private static final int EXIT_SUCCESS = 0;
  private static final int EXIT_ERROR = -1;
  private static final String CMD_HELP = "-help";

  // Command: application
  private static final String CMD_APPLICATION = "-application";

  // Applications Delete By appIds
  protected final static UsageInfo APPLICATION_DELETE_BY_APPIDS_USAGE =
      new UsageInfo(
          "--deleteByAppIds <application_id_1,application_id_2,application_id_3>",
          "This command is used to delete the specified applications.");

  protected final static String
      APPLICATION_DELETE_BY_APPIDS_USAGE_EXAMPLE_DESC =
      "If we want to delete application_1440536969523_0001 and application_1440536969523_0002.";

  protected final static String APPLICATION_DELETE_BY_APPIDS_USAGE_EXAMPLE =
      "yarn routeradmin -application --deleteByAppIds application_1440536969523_0001 application_1440536969523_0002";

  // Applications Delete By subClusterId
  protected final static UsageInfo APPLICATION_DELETE_BY_CLUSTERID_USAGE =
      new UsageInfo(
          "--deleteByClusterId <subCluster_id>",
          "This command is used to delete all apps from the specified subCluster.");

  protected final static String
      APPLICATION_DELETE_BY_CLUSTERID_USAGE_EXAMPLE_DESC =
      "If we want to delete all apps from cluster1.";

  protected final static String
      APPLICATION_DELETE_BY_CLUSTERID_USAGE_EXAMPLE =
      "yarn routeradmin -application --deleteByClusterId cluster1";


  protected final static RouterCmdUsageInfos APPLICATION_USAGEINFOS = new RouterCmdUsageInfos()
      // application delete by appIds
      .addUsageInfo(APPLICATION_DELETE_BY_APPIDS_USAGE)
      .addExampleDescs(APPLICATION_DELETE_BY_APPIDS_USAGE.args, APPLICATION_DELETE_BY_APPIDS_USAGE_EXAMPLE_DESC)
      .addExample(APPLICATION_DELETE_BY_APPIDS_USAGE.args, APPLICATION_DELETE_BY_APPIDS_USAGE_EXAMPLE)

      // application delete by subClusterId
      .addUsageInfo(APPLICATION_DELETE_BY_CLUSTERID_USAGE)
      .addExampleDescs(APPLICATION_DELETE_BY_CLUSTERID_USAGE.args, APPLICATION_DELETE_BY_CLUSTERID_USAGE_EXAMPLE_DESC)
      .addExample(APPLICATION_DELETE_BY_CLUSTERID_USAGE.args, APPLICATION_DELETE_BY_CLUSTERID_USAGE_EXAMPLE);

  // delete application
  private static final String OPTION_DELETE_APPS_BY_APP_IDS = "deleteByAppIds";

  private static final String OPTION_DELETE_APPS_BY_CLUSTER_ID = "deleteByClusterId";

  protected final static Map<String, RouterCmdUsageInfos> ADMIN_USAGE =
      ImmutableMap.<String, RouterCmdUsageInfos>builder()
          // Command3: application
          .put(CMD_APPLICATION, APPLICATION_USAGEINFOS)
          .build();

  public RouterCLI() {
    super();
  }

  public RouterCLI(Configuration conf) {
    super(conf);
  }

  private static void buildHelpMsg(String cmd, StringBuilder builder) {
    RouterCmdUsageInfos routerUsageInfo = ADMIN_USAGE.get(cmd);

    if (routerUsageInfo == null) {
      return;
    }
    builder.append("[").append(cmd).append("]\n");

    if (!routerUsageInfo.helpInfos.isEmpty()) {
      builder.append("\t Description: \n");
      for (String helpInfo : routerUsageInfo.helpInfos) {
        builder.append("\t\t").append(helpInfo).append("\n\n");
      }
    }

    if (!routerUsageInfo.usageInfos.isEmpty()) {
      builder.append("\t UsageInfos: \n");
      for (UsageInfo usageInfo : routerUsageInfo.usageInfos) {
        builder.append("\t\t").append(usageInfo.args)
            .append(": ")
            .append("\n\t\t")
            .append(usageInfo.help).append("\n\n");
      }
    }

    if (MapUtils.isNotEmpty(routerUsageInfo.examples)) {
      builder.append("\t Examples: \n");
      int count = 1;
      for (Map.Entry<String, List<String>> example : routerUsageInfo.examples.entrySet()) {

        String keyCmd = example.getKey();
        builder.append("\t\t")
            .append("Cmd:").append(count)
            .append(". ").append(keyCmd)
            .append(": \n\n");

        // Print Command Description
        List<String> exampleDescs = routerUsageInfo.exampleDescs.get(keyCmd);
        if (CollectionUtils.isNotEmpty(exampleDescs)) {
          builder.append("\t\t").append("Cmd Requirement Description:\n");
          for (String value : exampleDescs) {
            String[] valueDescs = StringUtils.split(value, "\\");
            for (String valueDesc : valueDescs) {
              builder.append("\t\t").append(valueDesc).append("\n");
            }
          }
        }

        builder.append("\n");

        // Print Command example
        List<String> valueExamples = example.getValue();
        if (CollectionUtils.isNotEmpty(valueExamples)) {
          builder.append("\t\t").append("Cmd Examples:\n");
          for (String valueExample : valueExamples) {
            builder.append("\t\t").append(valueExample).append("\n");
          }
        }
        builder.append("\n");
        count++;
      }
    }
  }

  private static void printHelp() {
    StringBuilder summary = new StringBuilder();
    summary.append("routeradmin is the command to execute ")
        .append("YARN Federation administrative commands.\n")
        .append("The full syntax is: \n\n")
        .append("routeradmin\n");
    StringBuilder helpBuilder = new StringBuilder();
    System.out.println(summary);

    for (String cmdKey : ADMIN_USAGE.keySet()) {
      buildHelpMsg(cmdKey, helpBuilder);
      helpBuilder.append("\n");
    }

    helpBuilder.append("   -help [cmd]: Displays help for the given command or all commands")
        .append(" if none is specified.");
    System.out.println(helpBuilder);
    System.out.println();
    ToolRunner.printGenericCommandUsage(System.out);
  }

  protected ResourceManagerAdministrationProtocol createAdminProtocol()
      throws IOException {
    // Get the current configuration
    final YarnConfiguration conf = new YarnConfiguration(getConf());
    return ClientRMProxy.createRMProxy(conf, ResourceManagerAdministrationProtocol.class);
  }

  private static void buildUsageMsg(StringBuilder builder) {
    builder.append("routeradmin is only used in Yarn Federation Mode.\n");
    builder.append("Usage: routeradmin\n");
    for (String cmdKey : ADMIN_USAGE.keySet()) {
      buildHelpMsg(cmdKey, builder);
      builder.append("\n");
    }
    builder.append("   -help [cmd]\n");
  }

  private static void printUsage(String cmd) {
    StringBuilder usageBuilder = new StringBuilder();
    if (ADMIN_USAGE.containsKey(cmd)) {
      buildHelpMsg(cmd, usageBuilder);
    } else {
      buildUsageMsg(usageBuilder);
    }
    System.err.println(usageBuilder);
    ToolRunner.printGenericCommandUsage(System.err);
  }

  private int handleDeleteApplicationsByAppIds(String[] applications) {
    System.out.println("Delete Applications = " + Arrays.asList(applications));
    try {
      ResourceManagerAdministrationProtocol adminProtocol =
          createAdminProtocol();
      for (String application : applications) {
        try {
          DeleteFederationApplicationRequest request =
              DeleteFederationApplicationRequest.newInstance(application, null);
          DeleteFederationApplicationResponse response =
              adminProtocol.deleteFederationApplication(request);
          System.out.println(response.getMessage());
        } catch (Exception e) {
          System.out.println(
              "Delete app: " + application + " failed: " + e.toString());
        }
      }
      return EXIT_SUCCESS;
    } catch (Exception e) {
      System.out.println("handleDeleteApplications error." + e.toString());
      return EXIT_ERROR;
    }
  }

  private int handleDeleteApplicationsByClusterId(String subClusterId) {
    System.out
        .println("Request to delete all Applications from subClusterId: " +
            subClusterId);
    try {
      ResourceManagerAdministrationProtocol adminProtocol =
          createAdminProtocol();
      DeleteFederationApplicationRequest request =
          DeleteFederationApplicationRequest.newInstance(null, subClusterId);
      DeleteFederationApplicationResponse response =
          adminProtocol.deleteFederationApplication(request);
      System.out.println(response.getMessage());
      return EXIT_SUCCESS;
    } catch (Exception e) {
      System.out.println("handleDeleteApplications error." + e.toString());
      return EXIT_ERROR;
    }
  }

  private int handleApplication(String[] args)
      throws IOException, YarnException, ParseException {
    // Prepare Options.
    Options opts = new Options();
    opts.addOption("application", false,
        "We provide a set of commands to query and clean applications.");
    Option deleteAppsByAppIdsOpt =
        new Option(null, OPTION_DELETE_APPS_BY_APP_IDS, true,
            "We will clean up the provided application.");
    deleteAppsByAppIdsOpt.setValueSeparator(' ');
    deleteAppsByAppIdsOpt.setArgs(Option.UNLIMITED_VALUES);
    deleteAppsByAppIdsOpt.setArgName("Application ID");
    opts.addOption(deleteAppsByAppIdsOpt);

    Option deleteAppsByClusterIdOpt =
        new Option(null, OPTION_DELETE_APPS_BY_CLUSTER_ID, true,
            "We will clean up the provided application.");
    opts.addOption(deleteAppsByClusterIdOpt);

    // Parse command line arguments.
    CommandLine cliParser;
    try {
      cliParser = new GnuParser().parse(opts, args);
    } catch (MissingArgumentException ex) {
      System.out.println("Missing argument for options");
      printUsage(args[0]);
      return EXIT_ERROR;
    }

    if (cliParser.hasOption(OPTION_DELETE_APPS_BY_CLUSTER_ID)) {
      String subClusterId =
          cliParser.getOptionValue(OPTION_DELETE_APPS_BY_CLUSTER_ID);
      return handleDeleteApplicationsByClusterId(subClusterId);
    }

    if (cliParser.hasOption(OPTION_DELETE_APPS_BY_APP_IDS)) {
      String[] applications =
          cliParser.getOptionValues(OPTION_DELETE_APPS_BY_APP_IDS);
      return handleDeleteApplicationsByAppIds(applications);
    }

    return 0;
  }

  @Override
  public int run(String[] args) throws Exception {
    YarnConfiguration yarnConf = getConf() == null ?
        new YarnConfiguration() : new YarnConfiguration(getConf());
    boolean isFederationEnabled = yarnConf.getBoolean(YarnConfiguration.FEDERATION_ENABLED,
        YarnConfiguration.DEFAULT_FEDERATION_ENABLED);

    if (args.length < 1 || !isFederationEnabled) {
      printUsage(CMD_EMPTY);
      return EXIT_ERROR;
    }

    System.out.println("args: " + Arrays.toString(args));

    String cmd = args[0];
    System.out.println("cmd: " + cmd);

    if (CMD_HELP.equals(cmd)) {
      if (args.length > 1) {
        printUsage(args[1]);
      } else {
        printHelp();
      }
      return EXIT_SUCCESS;
    } else if (CMD_APPLICATION.equals(cmd)) {
      return handleApplication(args);
    } else {
      System.out.println("No related commands found.");
      printHelp();
    }

    return EXIT_SUCCESS;
  }

  static class RouterCmdUsageInfos {
    private List<UsageInfo> usageInfos;
    private List<String> helpInfos;
    private Map<String, List<String>> examples;
    protected Map<String, List<String>> exampleDescs;

    RouterCmdUsageInfos() {
      this.usageInfos = new ArrayList<>();
      this.helpInfos = new ArrayList<>();
      this.examples = new LinkedHashMap<>();
      this.exampleDescs = new LinkedHashMap<>();
    }

    public RouterCmdUsageInfos addUsageInfo(UsageInfo usageInfo) {
      this.usageInfos.add(usageInfo);
      return this;
    }

    public RouterCmdUsageInfos addHelpInfo(String helpInfo) {
      this.helpInfos.add(helpInfo);
      return this;
    }

    private RouterCmdUsageInfos addExample(String cmd, String example) {
      List<String> exampleList = this.examples.getOrDefault(cmd, new ArrayList<>());
      exampleList.add(example);
      this.examples.put(cmd, exampleList);
      return this;
    }

    private RouterCmdUsageInfos addExampleDescs(String cmd, String exampleDesc) {
      List<String> exampleDescList = this.exampleDescs.getOrDefault(cmd, new ArrayList<>());
      exampleDescList.add(exampleDesc);
      this.exampleDescs.put(cmd, exampleDescList);
      return this;
    }

    public Map<String, List<String>> getExamples() {
      return examples;
    }
  }

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(new RouterCLI(), args);
    System.exit(result);
  }

  @VisibleForTesting
  public Map<String, RouterCmdUsageInfos> getAdminUsage(){
    return ADMIN_USAGE;
  }
}