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

import com.microsoft.reef.runtime.common.evaluator.PIDStoreStartHandler;
import com.microsoft.reef.util.OSUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A runnable class that encapsulates a process.
 */
public final class RunnableProcess implements Runnable {

  private static final Logger LOG = Logger.getLogger(RunnableProcess.class.getName());

  private static final String STD_ERROR_FILE_NAME = "STDERR.txt";
  private static final String STD_OUT_FILE_NAME = "STDOUT.txt";

  private final List<String> command;
  private final String id;
  private boolean started = false;
  private boolean ended = false;
  private Process process;
  private final File folder;

  /**
   * @param command the command to execute.
   * @param id      The ID of the process. This is used to name files and in the logs created by this process.
   * @param folder  The folder in which this will store its stdout and stderr output
   */
  public RunnableProcess(final List<String> command, final String id, final File folder) {
    this.command = new ArrayList<>(command);
    this.id = id;
    this.folder = folder;
    assert (this.folder.isDirectory());
    this.folder.mkdirs();
  }

  @Override
  public void run() {
    if (isEnded() || isStarted()) {
      throw new IllegalStateException("The RunnableProcess can't be reused");
    }

    // Setup the stdout and stderr destinations.
    final File errFile = new File(folder, STD_ERROR_FILE_NAME);
    final File outFile = new File(folder, STD_OUT_FILE_NAME);


    // Launch the process
    try {
      LOG.log(Level.FINEST, "Launching process \"{0}\"\nSTDERR can be found in {1}\nSTDOUT can be found in {2}",
          new Object[]{this.id, errFile.getAbsolutePath(), outFile.getAbsolutePath()});
      this.process = new ProcessBuilder()
          .command(this.command)
          .directory(this.folder)
          .redirectError(errFile)
          .redirectOutput(outFile)
          .start();
      this.started = true;
    } catch (final IOException ex) {
      LOG.log(Level.SEVERE, "Unable to spawn process \"{0}\" wth command {1}\n Exception:{2}",
          new Object[]{this.id, this.command, ex});
    }
    // Wait for its completion
    try {
      final int returnCode = process.waitFor();
      this.ended = true;
      LOG.log(Level.FINEST, "Process \"{0}\" returned {1}", new Object[]{this.id, returnCode});
    } catch (final InterruptedException ex) {
      LOG.log(Level.SEVERE, "Interrupted while waiting for the process \"{0}\" to complete. Exception: {2}",
          new Object[]{this.id, ex});
    }
  }

  boolean isStarted() {
    return started;
  }

  boolean isEnded() {
    return ended;
  }

  /**
   * Cancels the running process if it is running.
   */
  public void cancel() {
    if (this.isStarted() && !this.isEnded()) {
      this.process.destroy();
    }
    if (this.isStarted() && !this.isEnded()) {
      LOG.log(Level.WARNING, "The child process survived Process.destroy()");
      if (OSUtils.isLinux()) {
        LOG.log(Level.WARNING, "Attempting to kill the process via the kill command line");
        try {
          final long pid = readPID();
          OSUtils.kill(pid);
        } catch (final IOException | InterruptedException e) {
          LOG.log(Level.SEVERE, "Unable to kill the process.", e);
        }
      }
    }
  }

  private long readPID() throws IOException {
    final String PIDFileName = this.folder.getAbsolutePath() + "/" + PIDStoreStartHandler.PID_FILE_NAME;
    try (final BufferedReader r = new BufferedReader(new FileReader(PIDFileName))) {
      return Long.valueOf(r.readLine());
    }
  }
}
