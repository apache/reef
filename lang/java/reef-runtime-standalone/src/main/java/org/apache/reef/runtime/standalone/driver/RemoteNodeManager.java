/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.runtime.standalone.driver;

import com.jcraft.jsch.*;
import org.apache.reef.driver.evaluator.EvaluatorProcess;
import org.apache.reef.runtime.common.driver.api.ResourceLaunchEvent;
import org.apache.reef.runtime.common.driver.api.ResourceReleaseEvent;
import org.apache.reef.runtime.common.driver.api.ResourceRequestEvent;
import org.apache.reef.runtime.common.driver.evaluator.pojos.State;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceAllocationEvent;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceEventImpl;
import org.apache.reef.runtime.common.driver.resourcemanager.RuntimeStatusEventImpl;
import org.apache.reef.runtime.common.files.FileResource;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.common.parameters.JVMHeapSlack;
import org.apache.reef.runtime.common.utils.RemoteManager;
import org.apache.reef.runtime.standalone.client.parameters.RootFolder;
import org.apache.reef.runtime.local.process.ReefRunnableProcessObserver;
import org.apache.reef.runtime.standalone.client.parameters.SshPortNum;
import org.apache.reef.runtime.yarn.driver.REEFEventHandlers;
import org.apache.reef.runtime.standalone.client.parameters.NodeFolder;
import org.apache.reef.runtime.standalone.client.parameters.NodeInfoSet;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.util.CollectionUtils;
import org.apache.reef.util.Optional;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Management module for remote nodes in standalone runtime.
 */
public final class RemoteNodeManager {

  private static final Logger LOG = Logger.getLogger(RemoteNodeManager.class.getName());

  private final ThreadGroup containerThreads = new ThreadGroup("SshContainerManagerThreadGroup");

  /**
   * Map from containerID -> SshProcessContainer.
   */
  private final Map<String, SshProcessContainer> containers = new HashMap<>();

  private final ConfigurationSerializer configurationSerializer;
  private final REEFFileNames fileNames;
  private final double jvmHeapFactor;
  private final REEFEventHandlers reefEventHandlers;
  private final String errorHandlerRID;
  private final Set<String> nodeInfoSet;
  private Iterator<String> nodeSetIterator;
  private final ReefRunnableProcessObserver processObserver;
  private final String rootFolder;
  private final String nodeFolder;
  private final int sshPortNum;

  @Inject
  RemoteNodeManager(final ConfigurationSerializer configurationSerializer,
                    final REEFFileNames fileNames,
                    final RemoteManager remoteManager,
                    final REEFEventHandlers reefEventHandlers,
                    final ReefRunnableProcessObserver processObserver,
                    @Parameter(JVMHeapSlack.class) final double jvmHeapSlack,
                    @Parameter(NodeInfoSet.class) final Set<String> nodeInfoSet,
                    @Parameter(RootFolder.class) final String rootFolder,
                    @Parameter(NodeFolder.class) final String nodeFolder,
                    @Parameter(SshPortNum.class) final int sshPortNum) {
    this.configurationSerializer = configurationSerializer;
    this.fileNames = fileNames;
    this.processObserver = processObserver;
    this.errorHandlerRID = remoteManager.getMyIdentifier();
    this.reefEventHandlers = reefEventHandlers;
    this.jvmHeapFactor = 1.0 - jvmHeapSlack;
    this.nodeInfoSet = nodeInfoSet;
    this.rootFolder = rootFolder;
    this.nodeFolder = nodeFolder;
    this.sshPortNum = sshPortNum;

    this.nodeSetIterator = this.nodeInfoSet.iterator();

    LOG.log(Level.FINEST, "Initialized RemoteNodeManager.");
  }

  private void release(final String containerID) {
    synchronized (this.containers) {
      final SshProcessContainer sshProcessContainer = this.containers.get(containerID);
      if (null != sshProcessContainer) {
        LOG.log(Level.INFO, "Releasing Container with containerId [{0}]", sshProcessContainer);
        if (sshProcessContainer.isRunning()) {
          sshProcessContainer.close();
        }
        this.containers.remove(containerID);
      } else {
        LOG.log(Level.INFO, "Ignoring release request for unknown containerID [{0}]", containerID);
      }
    }
  }

  void onResourceLaunchRequest(final ResourceLaunchEvent resourceLaunchEvent) {
    LOG.log(Level.INFO, "RemoteNodeManager:onResourceLaunchRequest");

    // connect to the remote node.
    final String remoteNode;
    try {
      synchronized (this.nodeSetIterator) {
        remoteNode = this.getNode();
      }
    } catch (Exception e) {
      throw new RuntimeException("Unable to get remote node", e);
    }
    final String username;
    final String hostname;
    if (remoteNode.indexOf('@') < 0) {
      username = System.getProperty("user.name");
      hostname = remoteNode;
    } else {
      username = remoteNode.substring(0, remoteNode.indexOf('@'));
      hostname = remoteNode.substring(remoteNode.indexOf('@') + 1, remoteNode.length());
    }
    final String userHomeDir = System.getProperty("user.home");
    final String privatekey = userHomeDir + "/.ssh/id_dsa";

    synchronized (this.containers) {
      try {
        final JSch remoteConnection = new JSch();
        remoteConnection.addIdentity(privatekey);
        final Session sshSession = remoteConnection.getSession(username, hostname, sshPortNum);

        final Properties jschConfig = new Properties();
        jschConfig.put("StrictHostKeyChecking", "no");
        sshSession.setConfig(jschConfig);

        try {
          sshSession.connect();
        } catch (JSchException ex) {
          throw new RuntimeException("Unable to connect to " + remoteNode + ". " +
              "Check your authorized_keys settings. It should contain the public key of " + privatekey, ex);
        }

        LOG.log(Level.FINEST, "Established connection with {0}", hostname);

        final SshProcessContainer sshProcessContainer = this.containers.get(resourceLaunchEvent.getIdentifier())
            .withRemoteConnection(sshSession, remoteNode);

        // Add the global files and libraries.
        sshProcessContainer.addGlobalFiles(this.fileNames.getGlobalFolder());
        sshProcessContainer.addLocalFiles(getLocalFiles(resourceLaunchEvent));

        // Make the configuration file of the evaluator.
        final File evaluatorConfigurationFile =
            new File(sshProcessContainer.getFolder(), fileNames.getEvaluatorConfigurationPath());

        try {
          this.configurationSerializer.toFile(resourceLaunchEvent.getEvaluatorConf(), evaluatorConfigurationFile);
        } catch (final IOException | BindException e) {
          throw new RuntimeException("Unable to write configuration.", e);
        }

        // Copy files to remote node
        final Channel channel = sshSession.openChannel("exec");
        final String mkdirCommand = "mkdir " + nodeFolder;
        ((ChannelExec) channel).setCommand(mkdirCommand);
        channel.connect();

        final List<String> copyCommand =
            new ArrayList<>(Arrays.asList("scp", "-r",
                sshProcessContainer.getFolder().toString(),
                remoteNode + ":~/" + nodeFolder + "/" + sshProcessContainer.getContainerID()));
        LOG.log(Level.INFO, "Copying files: {0}", copyCommand);
        final Process copyProcess = new ProcessBuilder(copyCommand).start();
        try {
          copyProcess.waitFor();
        } catch (final InterruptedException ex) {
          throw new RuntimeException("Copying Interrupted: ", ex);
        }

        final List<String> command = getLaunchCommand(resourceLaunchEvent, sshProcessContainer.getMemory());
        LOG.log(Level.FINEST, "Launching container: {0}", sshProcessContainer);
        sshProcessContainer.run(command);
      } catch (final JSchException | IOException ex) {
        LOG.log(Level.WARNING, "Failed to establish connection with {0}@{1}:\n Exception:{2}",
            new Object[]{username, hostname, ex});
      }
    }
  }

  private String getNode() {
    if (!nodeSetIterator.hasNext()) {
      nodeSetIterator = this.nodeInfoSet.iterator();
    }
    return nodeSetIterator.next().trim();
  }

  private static List<File> getLocalFiles(final ResourceLaunchEvent launchRequest) {
    final List<File> files = new ArrayList<>();  // Libraries local to this evaluator
    for (final FileResource frp : launchRequest.getFileSet()) {
      files.add(new File(frp.getPath()).getAbsoluteFile());
    }
    return files;
  }

  private List<String> getLaunchCommand(final ResourceLaunchEvent launchRequest,
                                        final int containerMemory) {
    final EvaluatorProcess process = launchRequest.getProcess()
        .setConfigurationFileName(this.fileNames.getEvaluatorConfigurationPath());

    if (process.isOptionSet()) {
      return process.getCommandLine();
    } else {
      return process
          .setMemory((int) (this.jvmHeapFactor * containerMemory))
          .getCommandLine();
    }
  }

  void onResourceRequest(final ResourceRequestEvent resourceRequestEvent) {
    final Optional<String> node = selectNode(resourceRequestEvent);
    final String nodeId;

    if (node.isPresent()) {
      nodeId = node.get();
    } else {
      // Allocate new container
      nodeId = this.getNode() + ":" + String.valueOf(sshPortNum);
    }

    final String processID = nodeId + "-" + String.valueOf(System.currentTimeMillis());
    final File processFolder = new File(this.rootFolder, processID);

    final SshProcessContainer sshProcessContainer = new SshProcessContainer(errorHandlerRID, nodeId, processID,
        processFolder, resourceRequestEvent.getMemorySize().get(), resourceRequestEvent.getVirtualCores().get(),
        null, this.fileNames, this.nodeFolder, this.processObserver, this.containerThreads);

    this.containers.put(processID, sshProcessContainer);

    final ResourceAllocationEvent alloc = ResourceEventImpl.newAllocationBuilder()
        .setIdentifier(processID)
        .setNodeId(nodeId)
        .setResourceMemory(resourceRequestEvent.getMemorySize().get())
        .setVirtualCores(resourceRequestEvent.getVirtualCores().get())
        .setRuntimeName("STANDALONE")
        .build();
    reefEventHandlers.onResourceAllocation(alloc);

    // set the status as RUNNING.
    updateRuntimeStatus();
  }

  void onResourceReleaseRequest(final ResourceReleaseEvent releaseRequest) {
    synchronized (this.containers) {
      LOG.log(Level.FINEST, "Release container: {0}", releaseRequest.getIdentifier());
      this.release(releaseRequest.getIdentifier());
    }
  }

  public synchronized void close() {
    synchronized (this.containers) {
      if (this.containers.isEmpty()) {
        LOG.log(Level.FINEST, "Clean shutdown with no outstanding containers.");
      } else {
        LOG.log(Level.WARNING, "Dirty shutdown with outstanding containers.");
        for (final SshProcessContainer c : this.containers.values()) {
          LOG.log(Level.WARNING, "Force shutdown of: {0}", c);
          c.close();
        }
      }
    }
  }

  private Optional<String> selectNode(final ResourceRequestEvent resourceRequestEvent) {
    if (CollectionUtils.isNotEmpty(resourceRequestEvent.getNodeNameList())) {
      for (final String nodeName : resourceRequestEvent.getNodeNameList()) {
        return Optional.of(nodeName);
      }
    }
    if (CollectionUtils.isNotEmpty(resourceRequestEvent.getRackNameList())) {
      for (final String nodeName : resourceRequestEvent.getRackNameList()) {
        return Optional.of(nodeName);
      }
    }
    return Optional.empty();
  }

  private synchronized void updateRuntimeStatus() {
    final RuntimeStatusEventImpl.Builder builder = RuntimeStatusEventImpl.newBuilder()
        .setName("STANDALONE")
        .setState(State.RUNNING);

    this.reefEventHandlers.onRuntimeStatus(builder.build());
  }
}
