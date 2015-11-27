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
package org.apache.reef.runtime.local.process;

import org.apache.reef.runtime.common.evaluator.PIDStoreStartHandler;
import org.apache.reef.util.OSUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * A runnable class that encapsulates a process.
 */
public final class RunnableProcess implements Runnable {

  private static final Logger LOG = Logger.getLogger(RunnableProcess.class.getName());

  private static final long DESTROY_WAIT_TIME = 100;

  /**
   * Name of the file used for STDERR redirection.
   */
  private final String standardErrorFileName;
  /**
   * Name of the file used for STDOUT redirection.
   */
  private final String standardOutFileName;

  /**
   * Command to execute.
   */
  private final List<String> command;
  /**
   * User supplied ID of this process.
   */
  private final String id;
  /**
   * The working folder in which the process runs. It is also where STDERR and STDOUT files will be deposited.
   */
  private final File folder;
  /**
   * The coarse-grained lock for state transition.
   */
  private final Lock stateLock = new ReentrantLock();
  private final Condition doneCond = stateLock.newCondition();
  /**
   * This will be informed of process start and stop.
   */
  private final RunnableProcessObserver processObserver;
  /**
   * The process.
   */
  private Process process;
  /**
   * The state of the process.
   */
  private State state = State.INIT;   // synchronized on stateLock

  /**
   * @param command               the command to execute.
   * @param id                    The ID of the process. This is used to name files and in the logs created
   *                              by this process.
   * @param folder                The folder in which this will store its stdout and stderr output
   * @param processObserver       will be informed of process state changes.
   * @param standardOutFileName   The name of the file used for redirecting STDOUT
   * @param standardErrorFileName The name of the file used for redirecting STDERR
   */
  public RunnableProcess(final List<String> command,
                         final String id,
                         final File folder,
                         final RunnableProcessObserver processObserver,
                         final String standardOutFileName,
                         final String standardErrorFileName) {
    this.processObserver = processObserver;
    this.command = new ArrayList<>(command);
    this.id = id;
    this.folder = folder;
    assert this.folder.isDirectory();
    this.folder.mkdirs();
    this.standardOutFileName = standardOutFileName;
    this.standardErrorFileName = standardErrorFileName;
    LOG.log(Level.FINEST, "RunnableProcess ready.");
  }

  /**
   * Checks whether a transition from State 'from' to state 'to' is legal.
   *
   * @param from
   * @param to
   * @return true, if the state transition is legal. False otherwise.
   */
  private static boolean isLegal(final State from, final State to) {
    switch (from) {
    case INIT:
      switch (to) {
      case INIT:
      case RUNNING:
      case ENDED:
        return true;
      default:
        return false;
      }
    case RUNNING:
      switch (to) {
      case ENDED:
        return true;
      default:
        return false;
      }
    case ENDED:
      return false;
    default:
      return false;
    }
  }

  /**
   * Runs the configured process.
   *
   * @throws java.lang.IllegalStateException if the process is already running or has been running before.
   */
  @Override
  public void run() {
    this.stateLock.lock();
    try {
      if (this.getState() != State.INIT) {
        throw new IllegalStateException("The RunnableProcess can't be reused");
      }

      // Setup the stdout and stderr destinations.
      final File errFile = new File(folder, standardErrorFileName);
      final File outFile = new File(folder, standardOutFileName);

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
        this.setState(State.RUNNING);
        this.processObserver.onProcessStarted(this.id);
      } catch (final IOException ex) {
        LOG.log(Level.SEVERE, "Unable to spawn process \"{0}\" wth command {1}\n Exception:{2}",
            new Object[]{this.id, this.command, ex});
      }
    } finally {
      this.stateLock.unlock();
    }

    try {
      // Wait for its completion
      final int returnValue = process.waitFor();
      this.processObserver.onProcessExit(this.id, returnValue);
      this.stateLock.lock();
      try {
        this.setState(State.ENDED);
        this.doneCond.signalAll();
      } finally {
        this.stateLock.unlock();
      }
      LOG.log(Level.FINEST, "Process \"{0}\" returned {1}", new Object[]{this.id, returnValue});
    } catch (final InterruptedException ex) {
      LOG.log(Level.SEVERE, "Interrupted while waiting for the process \"{0}\" to complete. Exception: {2}",
          new Object[]{this.id, ex});
    }
  }


  /**
   * Cancels the running process if it is running.
   */
  public void cancel() {
    this.stateLock.lock();
    try {
      if (this.processIsRunning()) {
        this.process.destroy();
        this.doneCond.await(DESTROY_WAIT_TIME, TimeUnit.MILLISECONDS);
      }

      if (this.processIsRunning()) {
        LOG.log(Level.WARNING, "The child process survived Process.destroy()");
        if (OSUtils.isUnix() || OSUtils.isWindows()) {
          LOG.log(Level.WARNING, "Attempting to kill the process via the kill command line");
          try {
            final long pid = readPID();
            OSUtils.kill(pid);
          } catch (final IOException | InterruptedException | NumberFormatException e) {
            LOG.log(Level.SEVERE, "Unable to kill the process.", e);
          }
        }
      }

    } catch (final InterruptedException ex) {
      LOG.log(Level.SEVERE, "Interrupted while waiting for the process \"{0}\" to complete. Exception: {2}",
          new Object[]{this.id, ex});
    } finally {
      this.stateLock.unlock();
    }
  }

  /**
   * @return the PID stored in the PID file.
   * @throws IOException if the file can't be read.
   */
  private long readPID() throws IOException {
    final String pidFileName = this.folder.getAbsolutePath() + "/" + PIDStoreStartHandler.PID_FILE_NAME;
    try (final BufferedReader r =
             new BufferedReader(new InputStreamReader(new FileInputStream(pidFileName), StandardCharsets.UTF_8))) {
      return Long.parseLong(r.readLine());
    }
  }

  private boolean processIsRunning() {
    return this.getState() == State.RUNNING;
  }

  /**
   * @return the current State of the process.
   */
  private State getState() {
    return this.state;
  }

  /**
   * Sets a new state for the process.
   *
   * @param newState
   * @throws java.lang.IllegalStateException if the new state is illegal.
   */
  private void setState(final State newState) {
    if (!isLegal(this.state, newState)) {
      throw new IllegalStateException("Transition from " + this.state + " to " + newState + " is illegal");
    }
    this.state = newState;
  }

  /**
   * The possible states of a process: INIT, RUNNING, ENDED.
   */
  private enum State {
    // After initialization
    INIT,
    // The process is running
    RUNNING,
    // The process ended
    ENDED
  }
}
