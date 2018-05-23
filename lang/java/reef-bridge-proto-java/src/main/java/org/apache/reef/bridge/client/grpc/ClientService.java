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

package org.apache.reef.bridge.client.grpc;

import com.google.protobuf.ByteString;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.bridge.client.grpc.parameters.ClientServerPort;
import org.apache.reef.bridge.proto.*;
import org.apache.reef.bridge.proto.Void;
import org.apache.reef.client.*;
import org.apache.reef.exception.NonSerializableException;
import org.apache.reef.runtime.common.utils.ExceptionCodec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Client service.
 */
@ClientSide
@Unit
final class ClientService extends REEFClientGrpc.REEFClientImplBase implements AutoCloseable {

  private static final Logger LOG = Logger.getLogger(ClientService.class.getName());

  private final Object waitLock = new Object();

  private final InjectionFuture<REEF> reef;

  private final ExceptionCodec exceptionCodec;

  private Server server = null;

  private BridgeClientGrpc.BridgeClientFutureStub clientStub = null;

  private LauncherStatus status = LauncherStatus.INIT;

  private RunningJob theJob = null;

  /**
   * This class should not be instantiated.
   */
  @Inject
  private ClientService(
      @Parameter(ClientServerPort.class) final int clientServerPort,
      final ExceptionCodec exceptionCodec,
      final InjectionFuture<REEF> reef) {
    this.reef = reef;
    this.exceptionCodec = exceptionCodec;
    if (clientServerPort > 0) {
      try {
        this.server =  ServerBuilder.forPort(0)
            .addService(this)
            .build()
            .start();
        final ManagedChannel channel = ManagedChannelBuilder
            .forAddress("localhost", clientServerPort)
            .usePlaintext()
            .build();
        this.clientStub = BridgeClientGrpc.newFutureStub(channel);
        this.clientStub.registerREEFClient(
            ClientProtocol.REEFClientRegistration.newBuilder()
                .setHostname("localhost")
                .setPort(this.server.getPort())
                .build());
      } catch (IOException e) {
        throw new RuntimeException("unable to start gRPC server", e);
      }
    }
  }

  @Override
  public void close() {
    this.reef.get().close();
    if (this.server != null) {
      this.server.shutdown();
      this.server = null;
    }
  }

  public LauncherStatus submit(final Configuration driverServiceConfiguration) {
    this.reef.get().submit(driverServiceConfiguration);
    synchronized (this.waitLock) {
      while (!this.status.isDone()) {
        try {
          LOG.log(Level.FINE, "Wait indefinitely");
          this.waitLock.wait();
        } catch (final InterruptedException ex) {
          LOG.log(Level.FINE, "Interrupted: {0}", ex);
        }
      }
    }
    return this.status;
  }

  @Override
  public void driverControlHandler(
      final ClientProtocol.DriverControlOp request,
      final StreamObserver<Void> responseObserver) {
    try {
      if (this.theJob != null && this.theJob.getId().equals(request.getJobId())) {
        try {
          switch (request.getOperation()) {
          case CLOSE:
            if (request.getMessage().isEmpty()) {
              this.theJob.close();
            } else {
              this.theJob.close(request.getMessage().toByteArray());
            }
            break;
          case MESSAGE:
            this.theJob.send(request.getMessage().toByteArray());
            break;
          default:
            throw new RuntimeException("Unknown operation " + request.getOperation());
          }
        } catch (Exception e) {
          responseObserver.onError(Status.INTERNAL
              .withDescription(e.getMessage())
              .asRuntimeException());
        } finally {
          responseObserver.onNext(Void.newBuilder().build());
        }
      } else {
        responseObserver.onError(Status.INTERNAL
            .withDescription("Unknown job " + request.getJobId())
            .asRuntimeException());
      }
    } finally {
      responseObserver.onCompleted();
    }
  }

  private void setStatusAndNotify(final LauncherStatus launcherStatus) {
    synchronized (this.waitLock) {
      this.status = launcherStatus;
      this.waitLock.notify();
    }
  }

  private ByteString getRootCause(final Throwable cause) {
    Throwable t = cause;
    LOG.log(Level.INFO, "Exception class {0}", t.getClass().getName());
    while (t.getCause() != null) {
      t = t.getCause();
    }
    // Do we bottom out at a non-serializable (e.g., C#) exception?
    if (t instanceof NonSerializableException) {
      LOG.log(Level.INFO, "non serializable exception");
      return ByteString.copyFrom(((NonSerializableException)t).getError());
    } else {
      LOG.log(Level.INFO, "serializable exception found");
      return ByteString.copyFrom(exceptionCodec.toBytes(t));
    }
  }

  /**
   * Job message handler.
   */
  final class JobMessageHandler implements EventHandler<JobMessage> {
    @Override
    public void onNext(final JobMessage value) {
      LOG.log(Level.INFO, "Job message from id {0}", value.getId());
      if (clientStub != null) {
        clientStub.jobMessageHandler(ClientProtocol.JobMessageEvent.newBuilder()
            .setJobId(value.getId())
            .setMessage(ByteString.copyFrom(value.get()))
            .build());
      }
    }
  }

  /**
   * Job driver notifies us that the job has been submitted to the Resource Manager.
   */
  final class SubmittedJobHandler implements EventHandler<SubmittedJob> {
    @Override
    public void onNext(final SubmittedJob job) {
      LOG.log(Level.INFO, "REEF job submitted: {0}.", job.getId());
      setStatusAndNotify(LauncherStatus.SUBMITTED);
      if (clientStub != null) {
        clientStub.jobSumittedHandler(ClientProtocol.JobSubmittedEvent.newBuilder()
            .setJobId(job.getId())
            .build());
      }
    }
  }

  /**
   * Job driver notifies us that the job is running.
   */
  final class RunningJobHandler implements EventHandler<RunningJob> {
    @Override
    public void onNext(final RunningJob job) {
      LOG.log(Level.INFO, "The Job {0} is running.", job.getId());
      theJob = job;
      setStatusAndNotify(LauncherStatus.RUNNING);
      if (clientStub != null) {
        clientStub.jobRunningHandler(ClientProtocol.JobRunningEvent.newBuilder()
            .setJobId(job.getId())
            .build());
      }
    }
  }

  /**
   * Job driver notifies us that the job had failed.
   */
  final class FailedJobHandler implements EventHandler<FailedJob> {
    @Override
    public void onNext(final FailedJob job) {
      final Optional<Throwable> ex = job.getReason();
      LOG.log(Level.SEVERE, "Received an error for job " + job.getId(), ex);
      theJob = null;
      setStatusAndNotify(LauncherStatus.failed(ex));
      if (clientStub != null) {
        final Optional<Throwable> error = exceptionCodec.fromBytes(job.getData().get());
        final ExceptionInfo exception;
        if (error.isPresent()) {
          LOG.log(Level.SEVERE, "Error for " + job.getId(), ex);
          exception = ExceptionInfo.newBuilder()
              .setName(job.toString())
              .setMessage(job.getMessage())
              .setData(getRootCause(error.get()))
              .build();
        } else {
          LOG.log(Level.SEVERE, "Unserialized error for job {0}: {1}",
              new Object[] {job.getId(), job.getMessage()});
          exception = ExceptionInfo.newBuilder()
              .setName(job.toString())
              .setMessage(job.getMessage())
              .build();
        }
        clientStub.jobFailedHandler(ClientProtocol.JobFailedEvent.newBuilder()
            .setJobId(job.getId())
            .setException(exception)
            .build());
      }
    }
  }

  /**
   * Job driver notifies us that the job had completed successfully.
   */
  final class CompletedJobHandler implements EventHandler<CompletedJob> {
    @Override
    public void onNext(final CompletedJob job) {
      LOG.log(Level.INFO, "The Job {0} is done.", job.getId());
      theJob = null;
      setStatusAndNotify(LauncherStatus.COMPLETED);
      if (clientStub != null) {
        clientStub.jobCompletedHandler(ClientProtocol.JobCompletedEvent.newBuilder()
            .setJobId(job.getId())
            .build());
      }
    }
  }

  /**
   * Handler an error in the job driver.
   */
  final class RuntimeErrorHandler implements EventHandler<FailedRuntime> {
    @Override
    public void onNext(final FailedRuntime error) {
      LOG.log(Level.SEVERE, "Received a resource manager error", error.getReason());
      theJob = null;
      setStatusAndNotify(LauncherStatus.failed(error.getReason()));
      if (clientStub != null) {
        clientStub.runtimeErrorHandler(ExceptionInfo.newBuilder()
            .setMessage(error.getMessage())
            .setName(error.getId())
            .setData(error.getReason().isPresent() ?
                getRootCause(error.getReason().get()) : ByteString.EMPTY)
            .build());
      }
    }
  }

  /**
   * Wake error handler.
   */
  final class WakeErrorHandler implements EventHandler<Throwable> {
    @Override
    public void onNext(final Throwable value) {
      LOG.log(Level.SEVERE, "Received a wake error", value);
      if (clientStub != null) {
        clientStub.wakeErrorHandler(ExceptionInfo.newBuilder()
            .setName(value.toString())
            .setMessage(value.getMessage())
            .setData(getRootCause(value))
            .build());
      }

    }
  }
}
