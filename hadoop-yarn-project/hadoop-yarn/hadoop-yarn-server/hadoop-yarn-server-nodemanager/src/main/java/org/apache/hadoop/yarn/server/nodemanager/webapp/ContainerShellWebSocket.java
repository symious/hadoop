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

package org.apache.hadoop.yarn.server.nodemanager.webapp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.pty4j.PtyProcessBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ShellContainerCommand;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerExecContext;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.security.HadoopKerberosName;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pty4j.PtyProcess;
import com.pty4j.WinSize;

/**
 * Web socket for establishing interactive command shell connection through
 * Node Manage to container executor.
 */
@InterfaceAudience.LimitedPrivate({ "HDFS", "MapReduce", "YARN" })
@InterfaceStability.Unstable

@WebSocket
public class ContainerShellWebSocket {
  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerShellWebSocket.class);
  private static Context nmContext;

  private final ContainerExecutor exec;
  private IOStreamPair pair;

  private PtyProcess process;
  private String[] termCommand;
  private String shellPath;
  private File commandFile;
  private File profileFile;
  private Thread inThread;
  private Thread errThread;

  public ContainerShellWebSocket() {
    exec = nmContext.getContainerExecutor();
  }

  public static void init(Context nm) {
    ContainerShellWebSocket.nmContext = nm;
  }

  @OnWebSocketMessage
  public void onText(Session session, String message) throws IOException {
    try {
      if (session.isOpen()) {
        if (message.startsWith("RESIZE")) {
          int index = message.indexOf('{');
          String[] split =
              message.substring(index + 1, message.length() - 1).split(":");
          process.setWinSize(new WinSize(Integer.valueOf(split[0]),
              Integer.valueOf(split[1])));
        } else {
          // Send keystroke to process input
          byte[] payload;
          payload = message.getBytes(Charset.forName("UTF-8"));
          if (payload != null) {
            pair.out.write(payload);
            pair.out.flush();
          }
        }
      }
    } catch (IOException e) {
      onClose(session, 1001, "Shutdown");
    }

  }

  @OnWebSocketConnect
  public void onConnect(Session session) {
    try {
      URI containerURI = session.getUpgradeRequest().getRequestURI();
      String command = "sh";
      String[] containerPath = containerURI.getPath().split("/");
      String cId = containerPath[2];
      if (containerPath.length==4) {
        for (ShellContainerCommand c : ShellContainerCommand.values()) {
          if (c.name().equalsIgnoreCase(containerPath[3])) {
            command = containerPath[3].toLowerCase();
          }
        }
      }
      Container container = nmContext.getContainers().get(ContainerId
          .fromString(cId));
      LOG.info(session.getRemoteAddress().getHostString() + " connected!");
      LOG.info(
          "Making interactive connection to running docker container with ID: "
              + cId);
      ContainerExecContext execContext =
          new ContainerExecContext.Builder().setContainer(container)
              .setNMLocalPath(nmContext.getLocalDirsHandler()).setShell(command)
              .build();

      this.shellPath = nmContext.getConf()
          .get(YarnConfiguration.NM_WEB_TERMINAL_SHELL_PATH,
              "/usr/bin:/usr/local/bin:/etc/alternatives/bin");

      String commandFilePath = writeCommandToTempFile(execContext);
      this.termCommand = (PrivilegedOperationExecutor
          .getContainerExecutorExecutablePath(nmContext.getConf())
          + " --exec-container " + commandFilePath).split("\\s+");

      Map<String, String> envs = new HashMap<>(System.getenv());
      envs.put("TERM", "xterm");

      this.process =
          new PtyProcessBuilder().setCommand(termCommand).setEnvironment(envs)
              .start();
      pair = new IOStreamPair(process.getErrorStream(), process.getOutputStream());
      inThread = new Thread() {
        @Override
        public void run() {
          BufferedReader bufferedReader = new BufferedReader(
              new InputStreamReader(process.getInputStream()));
          try {
            int nRead;
            char[] data = new char[1 * 1024];

            while ((nRead = bufferedReader.read(data, 0, data.length)) != -1
                && !this.isInterrupted()) {
              StringBuilder builder = new StringBuilder(nRead);
              builder.append(data, 0, nRead);
              String result = builder.toString();
              session.getRemote().sendString(result);
            }
          } catch (IOException e) {
            onClose(session, 1001, "Shutdown");
          }
        }
      };
      inThread.start();

      errThread = new Thread() {
        @Override
        public void run() {
          BufferedReader bufferedReader = new BufferedReader(
              new InputStreamReader(process.getErrorStream()));
          try {
            int nRead;
            char[] data = new char[1 * 1024];

            while ((nRead = bufferedReader.read(data, 0, data.length)) != -1
                && !this.isInterrupted()) {
              StringBuilder builder = new StringBuilder(nRead);
              builder.append(data, 0, nRead);
              String result = builder.toString();
              session.getRemote().sendString(result);
            }
          } catch (IOException e) {
            onClose(session, 1001, "Shutdown");
          }
        }
      };
      errThread.start();

      pair.out.write(("source " + profileFile.getName() + "\n").getBytes());

    } catch (Exception e) {
      LOG.error("Failed to establish WebSocket connection with Client", e);
    }

  }

  @OnWebSocketClose
  public void onClose(Session session, int status, String reason) {
    try {
      LOG.info(session.getRemoteAddress().getHostString() + " closed!");
      String exit = "exit\r\n";
      pair.out.write(exit.getBytes(Charset.forName("UTF-8")));
      pair.out.flush();
      pair.in.close();
      pair.out.close();
      commandFile.delete();
      profileFile.delete();
      inThread.interrupt();
      errThread.interrupt();
    } catch (IOException e) {

    } finally {
      session.close();
    }
  }

  /**
   * Check if user is authorized to access container.
   * @param session websocket session
   * @param container instance of container to access
   * @return true if user is allowed to access container.
   * @throws IOException
   */
  protected boolean checkAuthorization(Session session, Container container)
      throws IOException {
    boolean authorized = true;
    String user = "";
    if (UserGroupInformation.isSecurityEnabled()) {
      user = new HadoopKerberosName(session.getUpgradeRequest()
          .getUserPrincipal().getName()).getShortName();
    } else {
      Map<String, List<String>> parameters = session.getUpgradeRequest()
          .getParameterMap();
      if (parameters.containsKey("user.name")) {
        List<String> users = parameters.get("user.name");
        user = users.get(0);
      }
    }
    boolean isAdmin = false;
    if (nmContext.getApplicationACLsManager().areACLsEnabled()) {
      UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
      isAdmin = nmContext.getApplicationACLsManager().isAdmin(ugi);
    }
    String containerUser = container.getUser();
    if (!user.equals(containerUser) && !isAdmin) {
      authorized = false;
    }
    return authorized;
  }

  private boolean checkInsecureSetup() {
    boolean kerberos = UserGroupInformation.isSecurityEnabled();
    boolean limitUsers = nmContext.getConf()
        .getBoolean(YarnConfiguration.NM_NONSECURE_MODE_LIMIT_USERS, true);
    if (kerberos) {
      return false;
    }
    return limitUsers;
  }

  private String writeCommandToTempFile(ContainerExecContext ctx)
      throws ContainerExecutionException {
    Container container = ctx.getContainer();
    File cmdDir = null;
    String appId = container.getContainerId().getApplicationAttemptId()
        .getApplicationId().toString();
    String containerId = container.getContainerId().toString();
    String filePrefix = containerId.toString();
    try {
      String cmdDirPath = ctx.getLocalDirsHandlerService().getLocalPathForWrite(
          ResourceLocalizationService.NM_PRIVATE_DIR + Path.SEPARATOR +
              appId + Path.SEPARATOR + filePrefix + Path.SEPARATOR).toString();
      cmdDir = new File(cmdDirPath);
      if (!cmdDir.mkdirs() && !cmdDir.exists()) {
        throw new IOException("Cannot create container private directory "
            + cmdDir);
      }
      profileFile = File.createTempFile(".webterminal", ".profile", cmdDir);
      Writer profileWriter = new OutputStreamWriter(
          new FileOutputStream(profileFile.toString()), "UTF-8");
      PrintWriter pw = new PrintWriter(profileWriter);
      pw.println("alias kill=\"printf 'command not supported\\n'\"");
      pw.println("alias rm=\"printf 'command not supported\\n'\"");
      pw.println("alias rmdir=\"printf 'command not supported\\n'\"");
      pw.println("alias mkdir=\"printf 'command not supported\\n'\"");
      pw.println("alias touch=\"printf 'command not supported\\n'\"");
      pw.println("alias yum=\"printf 'command not supported\\n'\"");
      pw.println("alias vim=\"vim -M\"");
      pw.println("alias vi=\"vi -M\"");
      pw.println("alias alias=\"printf ''\"");
      pw.flush();

      commandFile = File.createTempFile("yarn.",
          ".cmd", cmdDir);
      try (
          Writer writer = new OutputStreamWriter(
              new FileOutputStream(commandFile.toString()), "UTF-8");
          PrintWriter printWriter = new PrintWriter(writer);
      ) {
        Map<String, List<String>> cmd = new HashMap<String, List<String>>();
        // command = exec
        List<String> exec = new ArrayList<String>();
        exec.add("exec");
        cmd.put("command", exec);
        // user = foobar
        List<String> user = new ArrayList<String>();
        user.add("yarn");
        cmd.put("user", user);
        // launch-command = bash,-i
        List<String> commands = new ArrayList<String>();
        StringBuilder sb = new StringBuilder();
        sb.append("/bin/");
        sb.append(ctx.getShell());
        commands.add(sb.toString());
        commands.add("-ir");
        cmd.put("launch-command", commands);
        // workdir = ../nm-local-dir/usercache/appcache/appid/containerid
        List<String> workdir = new ArrayList<String>();
        workdir.add(container.getLogDir());
        cmd.put("workdir", workdir);
        List<String> pty = new ArrayList<String>();
        pty.add("true");
        cmd.put("use-pty", pty);
        List<String> pathEnv = new ArrayList<>();
        pathEnv.add(cmdDirPath + ":" + shellPath);
        cmd.put("pathenv", pathEnv);
        // generate cmd file
        printWriter.println("[command-execution]");
        for (Map.Entry<String, List<String>> entry :
            cmd.entrySet()) {
          if (entry.getKey().contains("=")) {
            throw new ContainerExecutionException(
                "'=' found in entry for docker command file, key = " + entry
                    .getKey() + "; value = " + entry.getValue());
          }
          if (entry.getValue().contains("\n")) {
            throw new ContainerExecutionException(
                "'\\n' found in entry for docker command file, key = " + entry
                    .getKey() + "; value = " + entry.getValue());
          }
          LOG.debug("key: " + entry.getKey() + " value: " + entry.getValue());
          printWriter.println("  " + entry.getKey() + "=" + StringUtils
              .join(",", entry.getValue()));
        }
        return commandFile.toString();
      }
    } catch (IOException e) {
      LOG.warn("Unable to write command to " + cmdDir);
      throw new ContainerExecutionException(e);
    }
  }
}