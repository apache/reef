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

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.annotations.audience.TaskSide;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.local.driver.*;
import org.apache.reef.runtime.local.process.ReefRunnableProcessObserver;
import org.apache.reef.runtime.local.process.RunnableProcess;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A Container that runs an Evaluator in a SshProcess.
 */
@Private
@TaskSide
final class SshProcessContainer implements Container {

  private static final Logger LOG = Logger.getLogger(SshProcessContainer.class.getName());

  private final String errorHandlerRID;
  private final String nodeID;
  private final File folder;
  private final String containedID;
  private final int megaBytes;
  private final int numberOfCores;
  private final String rackName;
  private final REEFFileNames fileNames;
  private final File localFolder;
  private final File globalFolder;
  private final ThreadGroup threadGroup;
  private Thread theThread;
  private final ReefRunnableProcessObserver processObserver;
  private RunnableProcess process;
  private Session remoteSession;
  private String remoteHostName;
  private final String nodeFolder;

  /**
   * @param errorHandlerRID the remoteID of the error handler.
   * @param nodeID          the ID of the (fake) node this Container is instantiated on
   * @param containedID     the  ID used to identify this container uniquely
   * @param folder          the folder in which logs etc. will be deposited.
   * @param nodeFolder      the folder in which the shaded jar file should be stored.
   */
  SshProcessContainer(final String errorHandlerRID,
                      final String nodeID,
                      final String containedID,
                      final File folder,
                      final int megaBytes,
                      final int numberOfCores,
                      final String rackName,
                      final REEFFileNames fileNames,
                      final String nodeFolder,
                      final ReefRunnableProcessObserver processObserver,
                      final ThreadGroup threadGroup) {

    this.errorHandlerRID = errorHandlerRID;
    this.processObserver = processObserver;
    this.nodeID = nodeID;
    this.containedID = containedID;
    this.folder = folder;
    this.megaBytes = megaBytes;
    this.numberOfCores = numberOfCores;
    this.rackName = rackName;
    this.fileNames = fileNames;
    this.nodeFolder = nodeFolder;
    this.threadGroup = threadGroup;

    final File reefFolder = new File(folder, fileNames.getREEFFolderName());

    this.localFolder = new File(reefFolder, fileNames.getLocalFolderName());
    if (!this.localFolder.exists() && !this.localFolder.mkdirs()) {
      LOG.log(Level.WARNING, "Failed to create [{0}]", this.localFolder.getAbsolutePath());
    }

    this.globalFolder = new File(reefFolder, fileNames.getGlobalFolderName());
    if (!this.globalFolder.exists() && !this.globalFolder.mkdirs()) {
      LOG.log(Level.WARNING, "Failed to create [{0}]", this.globalFolder.getAbsolutePath());
    }
  }

  @Override
  public void run(final List<String> commandLine) {
    this.process = new RunnableProcess(getRemoteCommand(commandLine),
        this.containedID,
        this.folder,
        this.processObserver,
        this.fileNames.getEvaluatorStdoutFileName(),
        this.fileNames.getEvaluatorStderrFileName());
    this.theThread = new Thread(this.threadGroup, this.process, this.containedID);
    this.theThread.start();
  }

  @Override
  public void addLocalFiles(final Iterable<File> files) {
    try {
      copy(files, this.localFolder);
    } catch (final IOException e) {
      throw new RuntimeException("Unable to copy files to the evaluator folder.", e);
    }
  }

  @Override
  public void addGlobalFiles(final File globalFilesFolder) {
    try {
      final File[] files = globalFilesFolder.listFiles();
      if (files != null) {
        copy(Arrays.asList(files), this.globalFolder);
      }
    } catch (final IOException e) {
      throw new RuntimeException("Unable to copy files to the evaluator folder.", e);
    }
  }

  private static void copy(final Iterable<File> files, final File folder) throws IOException {
    for (final File sourceFile : files) {
      final File destinationFile = new File(folder, sourceFile.getName());
      if (Files.isSymbolicLink(sourceFile.toPath())) {
        final Path linkTargetPath = Files.readSymbolicLink(sourceFile.toPath());
        Files.createSymbolicLink(destinationFile.toPath(), linkTargetPath);
      } else {
        Files.copy(sourceFile.toPath(), destinationFile.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
      }
    }
  }

  @Override
  public boolean isRunning() {
    return null != this.theThread && this.theThread.isAlive();
  }

  @Override
  public String getNodeID() {
    return this.nodeID;
  }

  @Override
  public String getContainerID() {
    return this.containedID;
  }

  @Override
  public int getMemory() {
    return this.megaBytes;
  }

  @Override
  public int getNumberOfCores() {
    return this.numberOfCores;
  }

  @Override
  public File getFolder() {
    return this.folder;
  }

  @Override
  public String getRackName() {
    return this.rackName;
  }

  @Override
  public void close() {
    if (isRunning()) {
      LOG.log(Level.WARNING, "Force-closing a container that is still running: {0}", this);
      this.process.cancel();
    }
  }

  @Override
  public String toString() {
    return "SshProcessContainer{" +
        "containedID='" + containedID + "'" +
        ", nodeID='" + nodeID + '\'' +
        ", errorHandlerRID='" + errorHandlerRID + '\'' +
        ", folder=" + folder + '\'' +
        ", rack=" + rackName +
        "}";
  }

  SshProcessContainer withRemoteConnection(final Session newRemoteSession, final String newRemoteHostName) {
    this.remoteSession = newRemoteSession;
    this.remoteHostName = newRemoteHostName;
    return this;
  }

  private List<String> getRemoteCommand(final List<String> commandLine) {
    final List<String> commandPrefix = new ArrayList<>(Arrays.asList("ssh", this.remoteHostName,
        "cd", this.getRemoteAbsolutePath(), "&&"));
    commandPrefix.addAll(commandLine);
    return commandPrefix;
  }

  private String getRemoteAbsolutePath() {
    return getRemoteHomePath() + "/" + this.nodeFolder + "/" + this.containedID;
  }

  private String getRemoteHomePath() {
    final String getHomeCommand = "pwd";
    try {
      final Channel channel = this.remoteSession.openChannel("exec");
      ((ChannelExec) channel).setCommand(getHomeCommand);
      channel.setInputStream(null);
      final InputStream stdout = channel.getInputStream();
      channel.connect();

      byte[] tmp = new byte[1024];
      StringBuilder homePath = new StringBuilder();
      while (true) {
        while (stdout.available() > 0) {
          final int len = stdout.read(tmp, 0, 1024);
          if (len < 0) {
            break;
          }
          homePath = homePath.append(new String(tmp, 0, len, StandardCharsets.UTF_8));
        }
        if (channel.isClosed()) {
          if (stdout.available() > 0) {
            continue;
          }
          break;
        }
      }
      return homePath.toString().trim();
    } catch (final JSchException | IOException ex) {
      throw new RuntimeException("Unable to retrieve home directory from " +
          this.remoteHostName + " with the pwd command", ex);
    }
  }
}
