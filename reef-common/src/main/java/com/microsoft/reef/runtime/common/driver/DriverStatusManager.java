package com.microsoft.reef.runtime.common.driver;

import com.google.protobuf.ByteString;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.runtime.common.driver.api.AbstractDriverRuntimeConfiguration;
import com.microsoft.reef.runtime.common.driver.client.ClientConnection;
import com.microsoft.reef.util.Optional;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;
import com.microsoft.wake.time.Clock;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages the Driver's status.
 */
public final class DriverStatusManager {
  private static final ObjectSerializableCodec<Throwable> EXCEPTION_CODEC = new ObjectSerializableCodec<>();
  private static final Logger LOG = Logger.getLogger(DriverStatusManager.class.getName());
  private final Clock clock;
  private final ClientConnection clientConnection;
  private final String jobIdentifier;
  private DriverStatus driverStatus = DriverStatus.PRE_INIT;
  private Optional<Throwable> shutdownCause = Optional.empty();
  private boolean driverTerminationHasBeenCommunicatedToClient = false;

  /**
   * @param clock
   * @param evaluators
   * @param clientConnection
   * @param jobIdentifier
   * @param remoteManager
   */
  @Inject
  public DriverStatusManager(final Clock clock,
                             final ClientConnection clientConnection,
                             final @Parameter(AbstractDriverRuntimeConfiguration.JobIdentifier.class) String jobIdentifier) {
    this.clock = clock;
    this.clientConnection = clientConnection;
    this.jobIdentifier = jobIdentifier;
    LOG.log(Level.INFO, "Instantiated 'DriverStatusManager'");
  }

  /**
   * Changes the driver status to INIT and sends message to the client about the transition.
   */
  public synchronized void onInit() {
    this.clientConnection.send(this.getInitMessage());
    this.setStatus(DriverStatus.INIT);
  }

  /**
   * Changes the driver status to RUNNING and sends message to the client about the transition.
   * If the driver is in status 'PRE_INIT', this first calls onInit();
   */
  public synchronized void onRunning() {
    if (this.driverStatus.equals(DriverStatus.PRE_INIT)) {
      this.onInit();
    }
    this.clientConnection.send(this.getRunningMessage());
    this.setStatus(DriverStatus.RUNNING);
  }

  /**
   * End the Driver with an exception.
   *
   * @param exception
   */
  public synchronized void onError(final Throwable exception) {
    LOG.log(Level.WARNING, "Shutting down the Driver with an exception: ", exception);
    this.shutdownCause = Optional.of(exception);
    this.clock.close();
    this.setStatus(DriverStatus.FAILING);
  }

  /**
   * Perform a clean shutdown of the Evaluator.
   */
  public synchronized void onComplete() {
    LOG.log(Level.INFO, "Clean shutdown of the Driver.");
    this.clock.close();
    this.setStatus(DriverStatus.SHUTTING_DOWN);
  }

  /**
   * Sends the final message to the Driver. This is used by DriverRuntimeStopHandler.onNext().
   *
   * @param exception
   */
  public synchronized void sendJobEndingMessageToClient(final Optional<Throwable> exception) {
    if (this.isNotShuttingDownOrFailing()) {
      LOG.log(Level.SEVERE, "Sending message in a state different that SHUTTING_DOWN or FAILING. This is likely a illegal call to clock.close() at play. Current state: " + this.driverStatus);
    }
    if (this.driverTerminationHasBeenCommunicatedToClient) {
      LOG.log(Level.SEVERE, ".sendJobEndingMessageToClient() called twice. Ignoring the second call");
    } else {
      { // Log the shutdown situation
        if (this.shutdownCause.isPresent()) {
          LOG.log(Level.WARNING, "Sending message about an unclean driver shutdown.", this.shutdownCause.get());
        }
        if (exception.isPresent()) {
          LOG.log(Level.WARNING, "There was an exception during clock.close().", exception.get());
        }
        if (this.shutdownCause.isPresent() && exception.isPresent()) {
          LOG.log(Level.WARNING, "The driver is shutdown because of an exception (see above) and there was an exception during clock.close(). Only the first exception will be sent to the client");
        }
      }
      if (this.shutdownCause.isPresent()) {
        // Send the earlier exception, if there was one
        this.clientConnection.send(getJobEndingMessage(this.shutdownCause));
      } else {
        // Send the exception passed, if there was one.
        this.clientConnection.send(getJobEndingMessage(exception));
      }
      this.driverTerminationHasBeenCommunicatedToClient = true;
    }
  }

  private synchronized boolean isShuttingDownOrFailing() {
    return DriverStatus.SHUTTING_DOWN.equals(this.driverStatus)
        || DriverStatus.FAILING.equals(this.driverStatus);
  }

  private synchronized boolean isNotShuttingDownOrFailing() {
    return !isShuttingDownOrFailing();
  }

  /**
   * Helper method to set the status. This also checks whether the transition from the current status to the new one is
   * legal.
   *
   * @param newStatus
   */
  private synchronized void setStatus(final DriverStatus newStatus) {
    if (isLegalTransition(this.driverStatus, newStatus)) {
      this.driverStatus = newStatus;
    } else {
      LOG.log(Level.WARNING, "Illegal state transiton: '" + this.driverStatus + "'->'" + newStatus + "'");
    }
  }


  /**
   * @param exception the exception that ended the Driver, if any.
   * @return message to be sent to the client at the end of the job.
   */
  private synchronized ReefServiceProtos.JobStatusProto getJobEndingMessage(final Optional<Throwable> exception) {
    final ReefServiceProtos.JobStatusProto message;
    if (exception.isPresent()) {
      message = ReefServiceProtos.JobStatusProto.newBuilder()
          .setIdentifier(this.jobIdentifier)
          .setState(ReefServiceProtos.State.FAILED)
          .setException(ByteString.copyFrom(EXCEPTION_CODEC.encode(exception.get())))
          .build();
    } else {
      message = ReefServiceProtos.JobStatusProto.newBuilder()
          .setIdentifier(this.jobIdentifier)
          .setState(ReefServiceProtos.State.DONE)
          .build();
    }
    return message;
  }

  /**
   * @return The message to be sent through the ClientConnection when in state INIT.
   */
  private synchronized ReefServiceProtos.JobStatusProto getInitMessage() {
    return ReefServiceProtos.JobStatusProto.newBuilder()
        .setIdentifier(this.jobIdentifier)
        .setState(ReefServiceProtos.State.INIT)
        .build();
  }

  /**
   * @return The message to be sent through the ClientConnection when in state RUNNING.
   */
  private synchronized ReefServiceProtos.JobStatusProto getRunningMessage() {
    return ReefServiceProtos.JobStatusProto.newBuilder()
        .setIdentifier(this.jobIdentifier)
        .setState(ReefServiceProtos.State.RUNNING)
        .build();
  }

  /**
   * Check whether a state transition 'from->to' is legal.
   *
   * @param from
   * @param to
   * @return
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
}
