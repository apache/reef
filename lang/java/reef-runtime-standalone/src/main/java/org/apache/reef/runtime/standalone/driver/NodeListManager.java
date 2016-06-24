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
import org.apache.reef.client.FailedRuntime;
import org.apache.reef.driver.evaluator.EvaluatorProcess;
import org.apache.reef.proto.ReefServiceProtos;
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
import org.apache.reef.runtime.local.client.parameters.RootFolder;
import org.apache.reef.runtime.yarn.driver.REEFEventHandlers;
import org.apache.reef.runtime.standalone.client.parameters.NodeFolder;
import org.apache.reef.runtime.standalone.client.parameters.NodeInfoSet;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.util.CollectionUtils;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.RemoteMessage;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Management module for remote nodes in standalone runtime.
 */
public final class NodeListManager {
  private static final Logger LOG = Logger.getLogger(NodeListManager.class.getName());

  /**
   * Map from containerID -> Container.
   */
  private final Map<String, Container> containers = new HashMap<>();

  private final Set<String> nodeInfoSet;
  private Iterator<String> nodeSetIterator;
  private final ConfigurationSerializer configurationSerializer;
  private final REEFFileNames fileNames;
  private final double jvmHeapFactor;
  private final REEFEventHandlers reefEventHandlers;
  private final String errorHandlerRID;
  private final String rootFolder;
  private final String nodeFolder;
  private final int sshPort = 22;

  @Inject
  NodeListManager(@Parameter(NodeInfoSet.class) final Set<String> nodeInfoSet,
                  final ConfigurationSerializer configurationSerializer,
                  final REEFFileNames fileNames,
                  @Parameter(JVMHeapSlack.class) final double jvmHeapSlack,
                  final RemoteManager remoteManager,
                  final REEFEventHandlers reefEventHandlers,
                  @Parameter(RootFolder.class) final String rootFolder,
                  @Parameter(NodeFolder.class) final String nodeFolder) {
    this.nodeInfoSet = nodeInfoSet;
    this.configurationSerializer = configurationSerializer;
    this.fileNames = fileNames;
    this.jvmHeapFactor = 1.0 - jvmHeapSlack;
    this.reefEventHandlers = reefEventHandlers;
    this.errorHandlerRID = remoteManager.getMyIdentifier();
    this.rootFolder = rootFolder;
    this.nodeFolder = nodeFolder;

    this.nodeSetIterator = this.nodeInfoSet.iterator();

    remoteManager.registerHandler(ReefServiceProtos.RuntimeErrorProto.class,
        new EventHandler<RemoteMessage<ReefServiceProtos.RuntimeErrorProto>>() {
          @Override
          public void onNext(final RemoteMessage<ReefServiceProtos.RuntimeErrorProto> value) {
            final FailedRuntime error = new FailedRuntime(value.getMessage());
            LOG.log(Level.SEVERE, "FailedRuntime: " + error, error.getReason().orElse(null));
            release(error.getId());
          }
        });

    LOG.log(Level.FINEST, "Initialized NodeListManager.");
  }

  void release(final String containerID) {
    synchronized (this.containers) {
      final Container ctr = this.containers.get(containerID);
      if (null != ctr) {
        LOG.log(Level.INFO, "Releasing Container with containerId [{0}]", ctr);
        if (ctr.isRunning()) {
          ctr.close();
        }
        this.containers.remove(ctr.getContainerID());
      } else {
        LOG.log(Level.INFO, "Ignoring release request for unknown containerID [{0}]", containerID);
      }
    }
  }

  public void onResourceLaunchRequest(final ResourceLaunchEvent resourceLaunchEvent) {
    LOG.log(Level.INFO, "NodeListManager:onResourceLaunchRequest");

    final String remoteNode;
    try {
      synchronized (this.nodeSetIterator) {
        remoteNode = this.getNode();
      }
    } catch (Exception e) {
      throw new RuntimeException("Unable to get remote node", e);
    }
    final String username = remoteNode.substring(0, remoteNode.indexOf('@'));
    final String hostname = remoteNode.substring(remoteNode.indexOf('@') + 1);
    final String userHomeDir = System.getProperty("user.home");
    final String privatekey = userHomeDir + "/.ssh/id_dsa";

    synchronized (this.containers) {
      final JSch remoteConnection = new JSch();
      final Session sshSession;
      try {
        remoteConnection.addIdentity(privatekey);
        sshSession = remoteConnection.getSession(username, hostname, sshPort);

        final Properties jschConfig = new Properties();
        jschConfig.put("StrictHostKeyChecking", "no");
        sshSession.setConfig(jschConfig);

        try {
          sshSession.connect();
        } catch (JSchException ex) {
          throw new RuntimeException("Unable to connect to the session. Check your authorized_keys settings.", ex);
        }

        LOG.log(Level.FINEST, "Established connection with {0}", hostname);

        final Container c = this.containers.get(resourceLaunchEvent.getIdentifier());
        c.setRemoteConnection(sshSession, remoteNode);

        // Add the global files and libraries.
        c.addGlobalFiles(this.fileNames.getGlobalFolder());
        c.addLocalFiles(getLocalFiles(resourceLaunchEvent));

        // Make the configuration file of the evaluator.
        final File evaluatorConfigurationFile = new File(c.getFolder(), fileNames.getEvaluatorConfigurationPath());

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

        // TODO[JIRA-1137]: Make the file copy job non-blocking; share common files in a node; execute the code itself
        final List<String> copyCommand = new ArrayList<>(Arrays.asList("scp", "-r", c.getFolder().toString(),
            remoteNode + ":~/" + nodeFolder + "/" + c.getContainerID()));
        LOG.log(Level.INFO, "Copying files: {0}", copyCommand);
        Process copyProcess = new ProcessBuilder(copyCommand).start();
        try {
          copyProcess.waitFor();
        } catch (final InterruptedException ex) {
          LOG.log(Level.SEVERE, "Copying Interrupted: {0}", ex);
        }

        final List<String> command = getLaunchCommand(resourceLaunchEvent, c.getMemory());
        LOG.log(Level.FINEST, "Launching container: {0}", c);
        c.run(command);
      } catch (final JSchException | IOException ex) {
        LOG.log(Level.WARNING, "Failed to establish connection with {0}@{1}:\n Exception:{2}",
            new Object[]{username, hostname, ex});
      }
    }
  }

  public String getNode() {
    if (!nodeSetIterator.hasNext()) {
      nodeSetIterator = this.nodeInfoSet.iterator();
    }
    return nodeSetIterator.next();
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

  public void onResourceRequest(final ResourceRequestEvent resourceRequestEvent) {
    final Optional<String> node = selectNode(resourceRequestEvent);
    final String nodeId;

    if (node.isPresent()) {
      // If nodeId is not in NodeList, raise Runtime Exception
      nodeId = node.get();
    } else {
      // Allocate new container
      nodeId = this.getNode() + ":" + String.valueOf(sshPort);
    }

    final String processID = nodeId + "-" + String.valueOf(System.currentTimeMillis());
    final File processFolder = new File(this.rootFolder, processID);
    final Container c = new ProcessContainer(this.errorHandlerRID, nodeId,
        processID, processFolder, resourceRequestEvent.getMemorySize().get(),
        resourceRequestEvent.getVirtualCores().get(), this.fileNames, nodeFolder);
    this.containers.put(processID, c);
    final ResourceAllocationEvent alloc = ResourceEventImpl.newAllocationBuilder()
        .setIdentifier(processID)
        .setNodeId(nodeId)
        .setResourceMemory(resourceRequestEvent.getMemorySize().get())
        .setVirtualCores(resourceRequestEvent.getVirtualCores().get())
        .setRuntimeName("STANDALONE")
        .build();
    reefEventHandlers.onResourceAllocation(alloc);

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
        for (final Container c : this.containers.values()) {
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
