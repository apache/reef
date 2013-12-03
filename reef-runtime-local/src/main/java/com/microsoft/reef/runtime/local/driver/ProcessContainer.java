/**
 * Copyright (C) 2013 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.runtime.local.driver;

import com.microsoft.reef.annotations.audience.ActivitySide;
import com.microsoft.reef.annotations.audience.Private;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A Container that runs an Evaluator in a Process
 */
@Private
@ActivitySide
final class ProcessContainer implements Container {
  private static final Logger LOG = Logger.getLogger(ProcessContainer.class.getName());
  private final String errorHandlerRID;
  private final String nodeID;
  private Thread theThread;
  private RunnableProcess process;
  private final File folder;
  private final String containedID;

  /**
   * @param errorHandlerRID
   * @param nodeID          the ID of the (fake) node this Container is instantiated on
   * @param containedID     the  ID used to identify this container uniquely
   * @param folder          the folder in which logs etc. will be deposited
   */
  ProcessContainer(final String errorHandlerRID, final String nodeID, final String containedID, final File folder) {
    this.errorHandlerRID = errorHandlerRID;
    this.nodeID = nodeID;
    this.containedID = containedID;
    this.folder = folder;
  }

  @Override
  public void addFiles(final Iterable<File> files) {
    try {
      copy(files, this.folder);
    } catch (IOException e) {
      throw new RuntimeException("Unable to copy files to the evaluator folder.", e);
    }
  }

  @Override
  public void run(List<String> commandLine) {
    this.process = new RunnableProcess(commandLine, containedID, this.folder);
    this.theThread = new Thread(process);
    this.theThread.start();
  }


  @Override
  public final boolean isRunning() {
    return null != this.theThread && this.theThread.isAlive();
  }

  @Override
  public final int getMemory() {
    return 512;
  }

  @Override
  public File getFolder() {
    return this.folder;
  }

  @Override
  public String getNodeID() {
    return nodeID;
  }

  @Override
  public String getContainerID() {
    return this.containedID;
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
    return "ProcessContainer{" +
        "containedID='" + containedID + '\'' +
        ", nodeID='" + nodeID + '\'' +
        ", errorHandlerRID='" + errorHandlerRID + '\'' +
        ", folder=" + folder +
        '}';
  }


  private static void copy(final Iterable<File> files, final File folder) throws IOException {
    for (final File sourceFile : files) {
      final File destinationFile = new File(folder, sourceFile.getName());
      Files.copy(sourceFile.toPath(), destinationFile.toPath());
    }
  }
}
