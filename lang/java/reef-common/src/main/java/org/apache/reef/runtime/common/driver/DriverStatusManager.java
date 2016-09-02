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
package org.apache.reef.runtime.common.driver;

import com.google.protobuf.ByteString;
import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.runtime.common.driver.client.ClientConnection;
import org.apache.reef.runtime.common.driver.parameters.JobIdentifier;
import org.apache.reef.runtime.common.utils.ExceptionCodec;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.time.Clock;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages the Driver's status.
 * Communicates status changes to the client and shuts down the runtime clock on shutdown.
 */
public final class DriverStatusManager {

  private static final Logger LOG = Logger.getLogger(DriverStatusManager.class.getName());
  private static final String CLASS_NAME = DriverStatusManager.class.getCanonicalName();

  private final Clock clock;
  private final ClientConnection clientConnection;
  private final String jobIdentifier;
  private final ExceptionCodec exceptionCodec;

  private DriverStatus driverStatus = DriverStatus.PRE_INIT;
  private Optional<Throwable> shutdownCause = Optional.empty();
  private boolean driverTerminationHasBeenCommunicatedToClient = false;

  /**
   * Build a new status manager. This is done automatically by Tang.
   * @param clock runtime event loop to shut down on completion or error.
   * @param clientConnection Connection to the job client. Send init, running, and job ending messages.
   * @param jobIdentifier String job ID.
   * @param exceptionCodec codec to serialize the exception when sending job ending message to the client.
   */
  @Inject
  private DriverStatusManager(
      @Parameter(JobIdentifier.class) final String jobIdentifier,
      final Clock clock,
      final ClientConnection clientConnection,
      final ExceptionCodec exceptionCodec) {

    LOG.entering(CLASS_NAME, "<init>");

    this.clock = clock;
    this.clientConnection = clientConnection;
    this.jobIdentifier = jobIdentifier;
    this.exceptionCodec = exceptionCodec;

    LOG.log(Level.FINE, "Instantiated 'DriverStatusManager'");

    LOG.exiting(CLASS_NAME, "<init>");
  }

  /**
   * Check whether a state transition 'from->to' is legal.
   * @param from Source state.
   * @param to Destination state.
   * @return true if transition is valid, false otherwise.
   */
  private static boolean isLegalTransition(final DriverStatus from, final DriverStatus to) {
    switch (from) {
    case PRE_INIT:
      switch (to) {
      case INIT:
        return true;
      default:
        return false;
      }
    case INIT:
      switch (to) {
      case RUNNING:
        return true;
      default:
        return false;
      }
    case RUNNING:
      switch (to) {
      case SHUTTING_DOWN:
      case FAILING:
        return true;
      default:
        return false;
      }
    case FAILING:
    case SHUTTING_DOWN:
      return false;
    default:
      throw new IllegalStateException("Unknown input state: " + from);
    }
  }

  /**
   * Changes the driver status to INIT and sends message to the client about the transition.
   */
  public synchronized void onInit() {

    LOG.entering(CLASS_NAME, "onInit");

    this.clientConnection.send(this.getInitMessage());
    this.setStatus(DriverStatus.INIT);

    LOG.exiting(CLASS_NAME, "onInit");
  }

  /**
   * Changes the driver status to RUNNING and sends message to the client about the transition.
   * If the driver is in status 'PRE_INIT', this first calls onInit();
   */
  public synchronized void onRunning() {

    LOG.entering(CLASS_NAME, "onRunning");

    if (this.driverStatus.equals(DriverStatus.PRE_INIT)) {
      this.onInit();
    }

    this.clientConnection.send(this.getRunningMessage());
    this.setStatus(DriverStatus.RUNNING);

    LOG.exiting(CLASS_NAME, "onRunning");
  }

  /**
   * End the Driver with an exception.
   * @param exception Exception that causes the driver shutdown.
   */
  public synchronized void onError(final Throwable exception) {

    LOG.entering(CLASS_NAME, "onError", exception);

    if (this.isShuttingDownOrFailing()) {
      LOG.log(Level.WARNING, "Received an exception while already in shutdown.", exception);
    } else {
      LOG.log(Level.WARNING, "Shutting down the Driver with an exception: ", exception);
      this.shutdownCause = Optional.of(exception);
      this.clock.stop(exception);
      this.setStatus(DriverStatus.FAILING);
    }

    LOG.exiting(CLASS_NAME, "onError", exception);
  }

  /**
   * Perform a clean shutdown of the Driver.
   */
  public synchronized void onComplete() {

    LOG.entering(CLASS_NAME, "onComplete");

    if (this.isShuttingDownOrFailing()) {
      LOG.log(Level.WARNING, "Ignoring second call to onComplete()",
          new Exception("Dummy exception to get the call stack"));
    } else {
      LOG.log(Level.INFO, "Clean shutdown of the Driver.");
      if (LOG.isLoggable(Level.FINEST)) {
        LOG.log(Level.FINEST, "Call stack: ",
            new Exception("Dummy exception to get the call stack"));
      }
      this.clock.close();
      this.setStatus(DriverStatus.SHUTTING_DOWN);
    }

    LOG.exiting(CLASS_NAME, "onComplete");
  }

  /**
   * Sends the final message to the client. This is used by DriverRuntimeStopHandler.onNext().
   * @param exception Exception that caused the job to end (optional).
   */
  public synchronized void onRuntimeStop(final Optional<Throwable> exception) {
    this.sendJobEndingMessageToClient(exception);
  }

  /**
   * Sends the final message to the Driver. This is used by DriverRuntimeStopHandler.onNext().
   * @param exception Exception that caused the job to end (can be absent).
   * @deprecated TODO[JIRA REEF-1548] Do not use DriverStatusManager as a proxy to the job client.
   * After release 0.16, make this method private and use it inside onRuntimeStop() method instead.
   */
  public synchronized void sendJobEndingMessageToClient(final Optional<Throwable> exception) {

    if (!this.isShuttingDownOrFailing()) {
      LOG.log(Level.SEVERE, "Sending message in a state different that SHUTTING_DOWN or FAILING. " +
          "This is likely a illegal call to clock.close() at play. Current state: {0}", this.driverStatus);
    }

    if (this.driverTerminationHasBeenCommunicatedToClient) {
      LOG.log(Level.SEVERE, ".sendJobEndingMessageToClient() called twice. Ignoring the second call");
      return;
    }

    // Log the shutdown situation
    if (this.shutdownCause.isPresent()) {
      LOG.log(Level.WARNING, "Sending message about an unclean driver shutdown.", this.shutdownCause.get());
    }

    if (exception.isPresent()) {
      LOG.log(Level.WARNING, "There was an exception during clock.close().", exception.get());
    }

    if (this.shutdownCause.isPresent() && exception.isPresent()) {
      LOG.log(Level.WARNING, "The driver is shutdown because of an exception (see above) and there was " +
          "an exception during clock.close(). Only the first exception will be sent to the client");
    }

    // Send the earlier exception, if there was one. Otherwise, send the exception passed.
    this.clientConnection.send(getJobEndingMessage(
        this.shutdownCause.isPresent() ? this.shutdownCause : exception));

    this.driverTerminationHasBeenCommunicatedToClient = true;
  }

  public synchronized boolean isShuttingDownOrFailing() {
    return DriverStatus.SHUTTING_DOWN.equals(this.driverStatus)
        || DriverStatus.FAILING.equals(this.driverStatus);
  }

  /**
   * Helper method to set the status.
   * This also checks whether the transition from the current status to the new one is legal.
   * @param newStatus Driver status to transition to.
   */
  private synchronized void setStatus(final DriverStatus newStatus) {
    if (isLegalTransition(this.driverStatus, newStatus)) {
      this.driverStatus = newStatus;
    } else {
      LOG.log(Level.WARNING, "Illegal state transition: {0} -> {1}", new Object[] {this.driverStatus, newStatus});
    }
  }

  /**
   * @param exception the exception that ended the Driver, if any.
   * @return message to be sent to the client at the end of the job.
   */
  private ReefServiceProtos.JobStatusProto getJobEndingMessage(final Optional<Throwable> exception) {
    if (exception.isPresent()) {
      return ReefServiceProtos.JobStatusProto.newBuilder()
          .setIdentifier(this.jobIdentifier)
          .setState(ReefServiceProtos.State.FAILED)
          .setException(ByteString.copyFrom(this.exceptionCodec.toBytes(exception.get())))
          .build();
    } else {
      return ReefServiceProtos.JobStatusProto.newBuilder()
          .setIdentifier(this.jobIdentifier)
          .setState(ReefServiceProtos.State.DONE)
          .build();
    }
  }

  /**
   * @return The message to be sent through the ClientConnection when in state INIT.
   */
  private ReefServiceProtos.JobStatusProto getInitMessage() {
    return ReefServiceProtos.JobStatusProto.newBuilder()
        .setIdentifier(this.jobIdentifier)
        .setState(ReefServiceProtos.State.INIT)
        .build();
  }

  /**
   * @return The message to be sent through the ClientConnection when in state RUNNING.
   */
  private ReefServiceProtos.JobStatusProto getRunningMessage() {
    return ReefServiceProtos.JobStatusProto.newBuilder()
        .setIdentifier(this.jobIdentifier)
        .setState(ReefServiceProtos.State.RUNNING)
        .build();
  }
}
