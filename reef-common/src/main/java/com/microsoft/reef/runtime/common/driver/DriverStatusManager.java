package com.microsoft.reef.runtime.common.driver;

import com.google.protobuf.ByteString;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.runtime.common.driver.api.AbstractDriverRuntimeConfiguration;
import com.microsoft.reef.runtime.common.driver.client.ClientConnection;
import com.microsoft.reef.runtime.common.driver.evaluator.Evaluators;
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
  private final Evaluators evaluators;
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
                             final Evaluators evaluators,
                             final ClientConnection clientConnection,
                             final @Parameter(AbstractDriverRuntimeConfiguration.JobIdentifier.class) String jobIdentifier) {
    this.clock = clock;
    this.evaluators = evaluators;
    this.clientConnection = clientConnection;
    this.jobIdentifier = jobIdentifier;

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
    this.closeClock();
    this.setStatus(DriverStatus.FAILED);
  }

  /**
   * Perform a clean shutdown of the Evaluator.
   */
  public synchronized void onComplete() {
    LOG.log(Level.INFO, "Clean shutdown of the Driver.");
    this.closeClock();
    this.setStatus(DriverStatus.COMPLETED);
  }


  private synchronized void closeClock() {
    this.clock.close();
  }


  /**
   * Sends the final message to the Driver. This is used by DriverRuntimeStopHandler.
   *
   * @param exception
   */
  public synchronized void sendJobEndingMessageToClient(final Optional<Throwable> exception) {
    if (!this.driverTerminationHasBeenCommunicatedToClient) {
      this.evaluators.close();

      if (this.shutdownCause.isPresent()) {
        // Send the earlier exception, if there was one
        this.clientConnection.send(getJobEndingMessage(this.shutdownCause));
      } else {
        // Send the exception passed, if there was one.
        this.clientConnection.send(getJobEndingMessage(exception));
      }
      this.driverTerminationHasBeenCommunicatedToClient = true;
    } else {
      LOG.log(Level.WARNING, ".sendJobEndingMessageToClient() called twice. Ignoring the second call");
    }
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
          case COMPLETED:
          case FAILED:
            return true;
          default:
            return false;
        }
      case FAILED:
      case COMPLETED:
        return false;
      default:
        throw new IllegalStateException("Unknown input state: " + from);
    }
  }
}
