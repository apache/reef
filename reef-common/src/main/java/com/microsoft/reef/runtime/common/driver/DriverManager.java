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
package com.microsoft.reef.runtime.common.driver;

import com.google.protobuf.ByteString;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.client.FailedRuntime;
import com.microsoft.reef.driver.catalog.NodeDescriptor;
import com.microsoft.reef.driver.catalog.RackDescriptor;
import com.microsoft.reef.driver.catalog.ResourceCatalog;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.reef.exception.EvaluatorException;
import com.microsoft.reef.proto.DriverRuntimeProtocol;
import com.microsoft.reef.proto.EvaluatorRuntimeProtocol;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.runtime.common.driver.api.AbstractDriverRuntimeConfiguration;
import com.microsoft.reef.runtime.common.driver.api.ResourceRequestHandler;
import com.microsoft.reef.runtime.common.driver.catalog.ResourceCatalogImpl;
import com.microsoft.reef.runtime.common.utils.RemoteManager;
import com.microsoft.reef.util.Optional;
import com.microsoft.tang.InjectionFuture;
import com.microsoft.tang.Injector;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.RemoteMessage;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;
import com.microsoft.wake.time.Clock;
import com.microsoft.wake.time.runtime.event.IdleClock;
import com.microsoft.wake.time.runtime.event.RuntimeStart;
import com.microsoft.wake.time.runtime.event.RuntimeStop;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The DriverManager responsibilities include the following:
 * <p/>
 * - Implement the EvaluatorRequestor interface
 * - House all EvaluatorManager instances
 * - Monitor the runtime for errors (via RuntimeErrorProto)
 * - Manage the clockFuture; including idle checks and shutdown
 * - Manage the *first contact* heartbeat message from the EvaluatorRuntime
 * - Manage the job control messages sent by the client (see JobControlHandler)
 * - Manage resource status updates from the resource manager (see ResourceStatusHandler)
 * - Manage resource allocations from the resource manager (see ResourceAllocationHandler)
 * - Manage all handlers associated with the ClientObserver
 * <p/>
 * To accomplish these tasks, the DriverManager makes use of the following:
 * <p/>
 * - A general heartbeat channel that receives (first contact) RemoteMessage<HeartbeatStatusProto> (evaluator -> driver)
 * - A general error channel that receives RemoteMessage<RuntimeErrorProto> values. (evaluator -> driver)
 * - EventHandlers that map to the ClientObserver API (client -> driver)
 * - EventHandler for sending runtime errors (driver -> client)
 * - EventHandler for requesting resources (driver -> resource manager)
 */
@Private
@Unit
final class DriverManager implements EvaluatorRequestor {

  private final static Logger LOG = Logger.getLogger(DriverManager.class.getName());

  private final Injector injector;
  private final InjectionFuture<Clock> clockFuture;
  private final ResourceCatalogImpl resourceCatalog;
  private final InjectionFuture<ResourceRequestHandler> futureResourceRequestHandler;
  private final Map<String, EvaluatorManager> evaluators = new HashMap<>();
  private final EvaluatorHeartBeatSanityChecker sanityChecker = new EvaluatorHeartBeatSanityChecker();
  private final ClientJobStatusHandler clientJobStatusHandler;
  private final AutoCloseable heartbeatConnectionChannel;
  private final AutoCloseable errorChannel;
  private final EventHandler<ReefServiceProtos.RuntimeErrorProto> runtimeErrorHandler;

  private DriverRuntimeProtocol.RuntimeStatusProto runtimeStatusProto =
      DriverRuntimeProtocol.RuntimeStatusProto.newBuilder()
          .setState(ReefServiceProtos.State.INIT)
          .setName("REEF")
          .setOutstandingContainerRequests(0)
          .build();

  @Inject
  DriverManager(
      final Injector injector,
      final ResourceCatalogImpl resourceCatalog,
      final RemoteManager remoteManager,
      final InjectionFuture<Clock> clockFuture,
      final InjectionFuture<ResourceRequestHandler> futureResourceRequestHandler,
      final ClientJobStatusHandler clientJobStatusHandler,
      final @Parameter(AbstractDriverRuntimeConfiguration.ClientRemoteIdentifier.class) String clientRID) {

    this.injector = injector;
    this.clockFuture = clockFuture;
    this.resourceCatalog = resourceCatalog;
    this.futureResourceRequestHandler = futureResourceRequestHandler;

    this.clientJobStatusHandler = clientJobStatusHandler;

    // Get the runtime handler for conveying errors to the client
    this.runtimeErrorHandler = remoteManager.getHandler(clientRID, ReefServiceProtos.RuntimeErrorProto.class);

    // General heartbeat channel for first contact evaluators
    this.heartbeatConnectionChannel = remoteManager.registerHandler(
        EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto.class,
        new EventHandler<RemoteMessage<EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto>>() {
          @Override
          public void onNext(final RemoteMessage<EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto> value) {
            final String evalId = value.getMessage().getEvaluatorStatus().getEvaluatorId();
            LOG.log(Level.FINEST, "TIME: Begin Heartbeat {0}", evalId);
            handle(value);
            LOG.log(Level.FINEST, "TIME: End Heartbeat {0}", evalId);
          }
        });

    // Runtime error channel for errors that occur on evaluators
    this.errorChannel = remoteManager.registerHandler(
        ReefServiceProtos.RuntimeErrorProto.class,
        new EventHandler<RemoteMessage<ReefServiceProtos.RuntimeErrorProto>>() {
          @Override
          public void onNext(RemoteMessage<ReefServiceProtos.RuntimeErrorProto> value) {
            handle(value.getMessage());
          }
        });

    LOG.log(Level.FINEST, "DriverManager instantiated");
  }

  @Override
  public void submit(final EvaluatorRequest req) {
    LOG.log(Level.FINEST, "Got an EvaluatorRequest");
    final DriverRuntimeProtocol.ResourceRequestProto.Builder request = DriverRuntimeProtocol.ResourceRequestProto.newBuilder();
    switch (req.getSize()) {
      case MEDIUM:
        request.setResourceSize(ReefServiceProtos.SIZE.MEDIUM);
        break;
      case LARGE:
        request.setResourceSize(ReefServiceProtos.SIZE.LARGE);
        break;
      case XLARGE:
        request.setResourceSize(ReefServiceProtos.SIZE.XLARGE);
        break;
      default:
        request.setResourceSize(ReefServiceProtos.SIZE.SMALL);
    }
    request.setResourceCount(req.getNumber());

    final ResourceCatalog.Descriptor descriptor = req.getDescriptor();
    if (descriptor != null) {
      if (descriptor instanceof RackDescriptor) {
        request.addRackName(descriptor.getName());
      } else if (descriptor instanceof NodeDescriptor) {
        request.addNodeName(descriptor.getName());
      }
    }

    this.futureResourceRequestHandler.get().onNext(request.build());
  }

  @Override
  public ResourceCatalog getResourceCatalog() {
    return this.resourceCatalog;
  }

  /**
   * Called from the EvaluatorManager to indicate its demise in some regard
   *
   * @param evaluatorManager calling me to say it is completely done.
   */
  final void release(final EvaluatorManager evaluatorManager) {
    synchronized (this.evaluators) {
      if (this.evaluators.containsKey(evaluatorManager.getId())) {
        this.evaluators.remove(evaluatorManager.getId());
      } else {
        throw new RuntimeException("Unknown evaluator manager: " + evaluatorManager.getId());
      }
    }
  }

  /**
   * Helper method to create a new EvaluatorManager instance
   *
   * @param id   identifier of the Evaluator
   * @param desc NodeDescriptor on which the Evaluator executes.
   * @return a new EvaluatorManager instance.
   */
  private final EvaluatorManager getNewEvaluatorManagerInstance(final String id, final NodeDescriptor desc) {
    // TODO: Make this to use the InjectorModule
    try {
      LOG.log(Level.FINEST, "Create Evaluator Manager: {0}", id);
      final Injector child = this.injector.createChildInjector();
      child.bindVolatileParameter(EvaluatorManager.EvaluatorIdentifier.class, id);
      child.bindVolatileParameter(EvaluatorManager.EvaluatorDescriptor.class, desc);
      return child.getInstance(EvaluatorManager.class);
    } catch (final BindException | InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Receives and routes heartbeats from Evaluators.
   *
   * @param evaluatorHeartbeatProtoRemoteMessage
   *
   */
  private final void handle(
      final RemoteMessage<EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto> evaluatorHeartbeatProtoRemoteMessage) {

    final EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto heartbeat = evaluatorHeartbeatProtoRemoteMessage.getMessage();
    final ReefServiceProtos.EvaluatorStatusProto status = heartbeat.getEvaluatorStatus();
    final String evaluatorId = status.getEvaluatorId();

    LOG.log(Level.FINEST, "Heartbeat from Evaluator {0} with state {1} timestamp {2}",
        new Object[] { evaluatorId, status.getState(), heartbeat.getTimestamp() });

    // Make sure that the timestamp we got for this evaluator is newer than the last one we got.
    this.sanityChecker.check(evaluatorId, heartbeat.getTimestamp());

    synchronized (this.evaluators) {
      if (this.evaluators.containsKey(evaluatorId)) {
        this.evaluators.get(evaluatorId).handle(evaluatorHeartbeatProtoRemoteMessage);
      } else {
        String msg = "Contact from unknown evaluator identifier " + evaluatorId;
        if (heartbeat.hasEvaluatorStatus()) {
          msg += " with state " + status.getState();
        }
        LOG.log(Level.SEVERE, msg);
        throw new RuntimeException(msg);
      }
    }
  }

  /**
   * This resource status message comes from the ResourceManager layer; telling me what it thinks
   * about the state of the resource executing an Evaluator; This method simply passes the message
   * off to the referenced EvaluatorManager
   *
   * @param resourceStatusProto resource status message from the ResourceManager
   */
  private final void handle(final DriverRuntimeProtocol.ResourceStatusProto resourceStatusProto) {
    synchronized (this.evaluators) {
      if (this.evaluators.containsKey(resourceStatusProto.getIdentifier())) {
        this.evaluators.get(resourceStatusProto.getIdentifier()).handle(resourceStatusProto);
      } else {
        throw new RuntimeException(
            "Unknown resource status from evaluator " + resourceStatusProto.getIdentifier() +
                " with state " + resourceStatusProto.getState());
      }
    }
  }

  /**
   * This method handles resource allocations by creating a new EvaluatorManager instance.
   *
   * @param resourceAllocationProto resource allocation from the ResourceManager
   */
  private final void handle(final DriverRuntimeProtocol.ResourceAllocationProto resourceAllocationProto) {
    synchronized (this.evaluators) {
      try {
        final NodeDescriptor nodeDescriptor = this.resourceCatalog.getNode(resourceAllocationProto.getNodeId());

        if (nodeDescriptor == null) {
          throw new RuntimeException("Unknown resource: " + resourceAllocationProto.getNodeId());
        }

        LOG.log(Level.FINEST, "Resource allocation: new evaluator id[{0}]", resourceAllocationProto.getIdentifier());
        final EvaluatorManager evaluator = getNewEvaluatorManagerInstance(resourceAllocationProto.getIdentifier(), nodeDescriptor);
        this.evaluators.put(resourceAllocationProto.getIdentifier(), evaluator);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * This method handles the runtime status information given by the ResourceManager layer.
   * It will check for an idle runtime status, and if the clockFuture is also idle, a system shutdown will be initiated.
   *
   * @param runtimeStatusProto runtime status from the ResourceManager layer
   */
  private final void handle(final DriverRuntimeProtocol.RuntimeStatusProto runtimeStatusProto) {
    final ReefServiceProtos.State runtimeState = runtimeStatusProto.getState();
    LOG.log(Level.FINEST, "Runtime status " + runtimeStatusProto);

    switch (runtimeState) {
      case FAILED:
        fail(runtimeStatusProto.getError());
        break;
      case DONE:
        this.clockFuture.get().close();
        break;
      case RUNNING:
        synchronized (this.evaluators) {
          this.runtimeStatusProto = runtimeStatusProto;
          if (this.clockFuture.get().isIdle()
              && runtimeStatusProto.getOutstandingContainerRequests() == 0
              && runtimeStatusProto.getContainerAllocationCount() == 0) {
            this.clockFuture.get().close();
          }
        }
        break;
    }
  }

  /**
   * This runtime error occurs on the evaluator
   */
  private final void handle(final ReefServiceProtos.RuntimeErrorProto runtimeErrorProto) {

    final FailedRuntime error = new FailedRuntime(runtimeErrorProto);
    LOG.log(Level.WARNING, "Runtime error: " + error, error.getCause());

    synchronized (this.evaluators) {
      if (evaluators.containsKey(error.getId())) {
        final EvaluatorException evaluatorException = error.getCause() != null ?
            new EvaluatorException(error.getId(), error.getCause()) :
            new EvaluatorException(error.getId(), "Runtime error");
        evaluators.get(error.getId()).handle(evaluatorException);
      } else {
        LOG.log(Level.WARNING, "Unknown evaluator runtime error: " + error, error.getCause());
      }
    }
  }

  /**
   * Something went wrong at the runtime layer (either driver or evaluator). This
   * method simply forwards the RuntimeErrorProto to the client via the RuntimeErrorHandler.
   *
   * @param runtimeErrorProto indicating what went wrong
   */
  private final void fail(final ReefServiceProtos.RuntimeErrorProto runtimeErrorProto) {
    this.runtimeErrorHandler.onNext(runtimeErrorProto);
    this.clockFuture.get().close();
  }

  /**
   * A RuntimeStatusProto comes from the ResourceManager layer indicating its current status
   */
  @Private
  final class RuntimeStatusHandler implements EventHandler<DriverRuntimeProtocol.RuntimeStatusProto> {
    @Override
    public void onNext(final DriverRuntimeProtocol.RuntimeStatusProto runtimeStatusProto) {
      handle(runtimeStatusProto);
    }
  }

  /**
   * A ResourceStatusProto message comes from the ResourceManager layer to indicate what it thinks
   * about the current state of a given resource. Ideally, we should think the same thing.
   */
  @Private
  final class ResourceStatusHandler implements EventHandler<DriverRuntimeProtocol.ResourceStatusProto> {
    @Override
    public void onNext(final DriverRuntimeProtocol.ResourceStatusProto value) {
      DriverManager.this.handle(value);
    }
  }

  /**
   * A ResourceAllocationProto indicates a resource allocation given by the ResourceManager layer.
   */
  @Private
  final class ResourceAllocationHandler implements EventHandler<DriverRuntimeProtocol.ResourceAllocationProto> {
    @Override
    public void onNext(final DriverRuntimeProtocol.ResourceAllocationProto value) {
      DriverManager.this.handle(value);
    }
  }

  /**
   * A NodeDescriptorProto defines a new node in the cluster. We should add this to the resource catalog
   * so that clients can make resource requests against it.
   */
  @Private
  final class NodeDescriptorHandler implements EventHandler<DriverRuntimeProtocol.NodeDescriptorProto> {
    @Override
    public void onNext(final DriverRuntimeProtocol.NodeDescriptorProto value) {
      DriverManager.this.resourceCatalog.handle(value);
    }
  }

  /**
   * This EventHandler is subscribed to the StartTime event of the Clock statically. It therefore provides the entrance
   * point to REEF.
   */
  @Private
  final class RuntimeStartHandler implements EventHandler<RuntimeStart> {
    @Override
    public void onNext(final RuntimeStart startTime) {
      LOG.log(Level.FINEST, "RuntimeStart: {0}", startTime);
      DriverManager.this.runtimeStatusProto = DriverRuntimeProtocol.RuntimeStatusProto.newBuilder()
          .setState(ReefServiceProtos.State.RUNNING)
          .setName("REEF")
          .setOutstandingContainerRequests(0)
          .build();
    }
  }

  /**
   * Handles RuntimeStop
   */
  @Private
  final class RuntimeStopHandler implements EventHandler<RuntimeStop> {
    @Override
    public void onNext(final RuntimeStop runtimeStop) {
      LOG.log(Level.FINEST, "RuntimeStop: {0}", runtimeStop);
      if (runtimeStop.getException() != null) {
        final ObjectSerializableCodec<Exception> codec = new ObjectSerializableCodec<>();
        final Exception exception = runtimeStop.getException();
        LOG.log(Level.WARNING, "Sending runtime error: {0}", exception.getMessage());
        DriverManager.this.runtimeErrorHandler.onNext(ReefServiceProtos.RuntimeErrorProto.newBuilder()
            .setMessage(exception.getMessage())
            .setException(ByteString.copyFrom(codec.encode(exception)))
            .setName("REEF")
            .build());
        LOG.log(Level.WARNING, "DONE Sending runtime error: {0}", exception.getMessage());
      }

      synchronized (DriverManager.this.evaluators) {
        // TODO: Log an unsafe shutdown
        for (final EvaluatorManager evaluatorManager : new ArrayList<>(DriverManager.this.evaluators.values())) {
          LOG.log(Level.WARNING, "Unclean shutdown of evaluator {0}", evaluatorManager.getId());
          evaluatorManager.close();
        }
      }

      try {
        DriverManager.this.heartbeatConnectionChannel.close();
        DriverManager.this.errorChannel.close();
        DriverManager.this.clientJobStatusHandler.close(runtimeStop.getException() != null ?
            Optional.<Throwable>of(runtimeStop.getException()) : Optional.<Throwable>empty());
        // TODO: Expose an event here that Runtime Local can subscribe to.
        LOG.log(Level.FINEST, "driver manager closed");
      } catch (final Exception ex) {
        LOG.log(Level.SEVERE, "Error closing Driver Manager", ex);
      }
    }
  }

  @Private
  final class IdleHandler implements EventHandler<IdleClock> {
    @Override
    public void onNext(final IdleClock idleClock) {
      LOG.log(Level.INFO, "IdleClock: {0}"
          + "\n\t Runtime state {1}"
          + "\n\t Outstanding container requests {2}"
          + "\n\t Container allocation count {3}",
          new Object[]{idleClock,
              runtimeStatusProto.getState(),
              runtimeStatusProto.getOutstandingContainerRequests(),
              runtimeStatusProto.getContainerAllocationCount()});

      synchronized (DriverManager.this.evaluators) {
        if (ReefServiceProtos.State.RUNNING == runtimeStatusProto.getState() &&
            0 == runtimeStatusProto.getOutstandingContainerRequests() &&
            0 == runtimeStatusProto.getContainerAllocationCount()) {
          LOG.log(Level.FINEST, "Idle runtime shutdown");
          DriverManager.this.clockFuture.get().close();
        }
      }
    }
  }
}
