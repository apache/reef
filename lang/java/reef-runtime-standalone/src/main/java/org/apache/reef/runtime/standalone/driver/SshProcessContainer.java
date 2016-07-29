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

import com.jcraft.jsch.Session;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.annotations.audience.TaskSide;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.local.process.ReefRunnableProcessObserver;
import org.apache.reef.runtime.standalone.process.SshProcess;

import java.io.File;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A Container that runs an Evaluator in a Process.
 */
@Private
@TaskSide
final class SshProcessContainer implements AutoCloseable {

  private static final Logger LOG = Logger.getLogger(SshProcessContainer.class.getName());

  private final File folder;
  private final String containedID;
  private final REEFFileNames fileNames;
  private Thread theThread;
  private final ReefRunnableProcessObserver processObserver;
  private SshProcess process;
  private Session remoteSession;
  private String remoteHostName;
  private final String nodeFolder;

  /**
   * @param containedID     the  ID used to identify this container uniquely
   * @param folder          the folder in which logs etc. will be deposited
   */
  SshProcessContainer(final String containedID,
                      final File folder,
                      final REEFFileNames fileNames,
                      final String nodeFolder,
                      final ReefRunnableProcessObserver processObserver) {
    this.processObserver = processObserver;
    this.containedID = containedID;
    this.folder = folder;
    this.fileNames = fileNames;
    this.nodeFolder = nodeFolder;
  }

  public void run(final List<String> commandLine) {
    this.process = new SshProcess(commandLine,
        this.containedID,
        this.folder,
        this.processObserver,
        this.fileNames.getEvaluatorStdoutFileName(),
        this.fileNames.getEvaluatorStderrFileName(),
        this.remoteSession,
        this.remoteHostName,
        this.nodeFolder);
    this.theThread = new Thread(this.process);
    this.theThread.start();
  }

  public boolean isRunning() {
    return null != this.theThread && this.theThread.isAlive();
  }

  public File getFolder() {
    return this.folder;
  }

  String getContainerID() {
    return this.containedID;
  }

  @Override
  public void close() {
    if (isRunning()) {
      LOG.log(Level.WARNING, "Force-closing a container that is still running: {0}", this);
      this.process.cancel();
    }
  }

  SshProcessContainer withRemoteConnection(final Session newRemoteSession, final String newRemoteHostName) {
    this.remoteSession = newRemoteSession;
    this.remoteHostName = newRemoteHostName;
    return this;
  }
}
