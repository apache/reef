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
 * Manages the Driver's shutdown.
 */
public final class DriverShutdownManager {
  private static final ObjectSerializableCodec<Throwable> CODEC = new ObjectSerializableCodec<>();
  private static final Logger LOG = Logger.getLogger(DriverShutdownManager.class.getName());
  private final Clock clock;
  private final Evaluators evaluators;
  private final ClientConnection clientConnection;
  private final String jobIdentifier;

  private boolean isShutdown;

  /**
   * @param clock
   * @param evaluators
   * @param clientConnection
   * @param jobIdentifier
   * @param remoteManager
   */
  @Inject
  public DriverShutdownManager(final Clock clock,
                               final Evaluators evaluators,
                               final ClientConnection clientConnection,
                               final @Parameter(AbstractDriverRuntimeConfiguration.JobIdentifier.class) String jobIdentifier) {
    this.clock = clock;
    this.evaluators = evaluators;
    this.clientConnection = clientConnection;
    this.jobIdentifier = jobIdentifier;

  }


  /**
   * End the Driver with an exception.
   *
   * @param exception
   */
  public synchronized void onError(final Throwable exception) {
    LOG.log(Level.WARNING, "Shutting down the Driver with an exception: ", exception);
    this.close(Optional.of(exception));


  }

  /**
   * Perform a clean shutdown of the Evaluator.
   */
  public synchronized void onComplete() {
    LOG.log(Level.INFO, "Clean shutdown of the Driver.");
    this.close(Optional.<Throwable>empty());
  }

  /**
   * @param exception
   */
  private synchronized void close(final Optional<Throwable> exception) {
    if (!this.isShutdown) {
      this.evaluators.close();
      this.clientConnection.send(getJobEndingProto(exception));
      this.clock.close();
      this.isShutdown = true;
    } else {
      LOG.log(Level.WARNING, ".close() called twice. Ignoring the second call");
    }
  }

  /**
   * @return true, iff the Driver has been shutdown before.
   */
  public synchronized boolean isShutdown() {
    return isShutdown;
  }

  /**
   * @param exception the exception that ended the Driver, if any.
   * @return message to be sent to the client at the end of the job.
   */
  private synchronized ReefServiceProtos.JobStatusProto getJobEndingProto(final Optional<Throwable> exception) {
    final ReefServiceProtos.JobStatusProto message;
    if (exception.isPresent()) {
      message = ReefServiceProtos.JobStatusProto.newBuilder()
          .setIdentifier(this.jobIdentifier)
          .setState(ReefServiceProtos.State.FAILED)
          .setException(ByteString.copyFrom(CODEC.encode(exception.get())))
          .build();
    } else {
      message = ReefServiceProtos.JobStatusProto.newBuilder()
          .setIdentifier(this.jobIdentifier)
          .setState(ReefServiceProtos.State.DONE)
          .build();
    }
    return message;
  }
}
