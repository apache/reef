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

import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.client.DriverConfigurationOptions;
import com.microsoft.reef.driver.catalog.NodeDescriptor;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.ClosedContext;
import com.microsoft.reef.driver.context.ContextMessage;
import com.microsoft.reef.driver.context.FailedContext;
import com.microsoft.reef.driver.evaluator.*;
import com.microsoft.reef.driver.task.*;
import com.microsoft.reef.exception.EvaluatorException;
import com.microsoft.reef.io.naming.Identifiable;
import com.microsoft.reef.proto.DriverRuntimeProtocol;
import com.microsoft.reef.proto.EvaluatorRuntimeProtocol;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.runtime.common.REEFErrorHandler;
import com.microsoft.reef.runtime.common.driver.api.ResourceLaunchHandler;
import com.microsoft.reef.runtime.common.driver.api.ResourceReleaseHandler;
import com.microsoft.reef.runtime.common.driver.evaluator.EvaluatorDescriptorImpl;
import com.microsoft.reef.runtime.common.utils.RemoteManager;
import com.microsoft.reef.util.Optional;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.formats.ConfigurationSerializer;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.RemoteMessage;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;
import com.microsoft.wake.time.Clock;
import com.microsoft.wake.time.event.Alarm;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages a single Evaluator instance including all lifecycle instances:
 * (AllocatedEvaluator, CompletedEvaluator, FailedEvaluator).
 * <p/>
 * A (periodic) heartbeat channel is established EvaluatorRuntime -> EvaluatorManager.
 * The EvaluatorRuntime will (periodically) send (status) messages to the EvaluatorManager using this
 * heartbeat channel.
 * <p/>
 * A (push-based) EventHandler channel is established EvaluatorManager -> EvaluatorRuntime.
 * The EvaluatorManager uses this to forward Driver messages, launch Tasks, and initiate
 * control information (e.g., shutdown, suspend).
 */
@Private
public final class EvaluatorManager implements Identifiable, AutoCloseable {

  // Dynamic Parameters
  @NamedParameter(doc = "The Evaluator Identifier.")
  final static class EvaluatorIdentifier implements Name<String> {
  }

  @NamedParameter(doc = "The Evaluator Host.")
  final static class EvaluatorDescriptorName implements Name<EvaluatorDescriptorImpl> {
  }

  private final static Logger LOG = Logger.getLogger(EvaluatorManager.class.getName());

  /**
   * Various states that the EvaluatorManager could be in. The EvaluatorManager is
   * created when a resource has been allocated by the ResourceManager.
   */
  static enum State {
    ALLOCATED,  // initial state
    SUBMITTED,  // client called AllocatedEvaluator.submitTask() and we're waiting for first contact
    RUNNING,    // first contact received, all communication channels established, Evaluator sent to client.
    // TODO: Add CLOSING state
    DONE,       // clean shutdown
    FAILED,     // some failure occurred.
    KILLED      // unclean shutdown
  }

  private State state = State.ALLOCATED;

  private final Clock clock;

  private final RemoteManager remoteManager;

  private final DriverManager driverManager;

  private final ResourceReleaseHandler resourceReleaseHandler;

  private final ResourceLaunchHandler resourceLaunchHandler;

  private final String evaluatorId;

  private final EvaluatorDescriptorImpl evaluatorDescriptor;

  private final List<EvaluatorContext> activeContextList = new ArrayList<>();

  private final Set<String> activeContextIds = new HashSet<>();

  private final DispatchingEStage dispatcher;

  private final ConfigurationSerializer configurationSerializer;

  // Mutable fields

  private RunningTask runningTask = null;

  private EventHandler<EvaluatorRuntimeProtocol.EvaluatorControlProto> evaluatorControlHandler = null;

  private boolean isResourceReleased = false;

  @Inject
  EvaluatorManager(
      final Clock clock,
      final RemoteManager remoteManager,
      final DriverManager driverManager,
      final ResourceReleaseHandler resourceReleaseHandler,
      final ResourceLaunchHandler resourceLaunchHandler,
      final REEFErrorHandler errorHandler,
      final @Parameter(EvaluatorIdentifier.class) String evaluatorId,
      final @Parameter(EvaluatorDescriptorName.class) EvaluatorDescriptorImpl evaluatorDescriptor,
      final @Parameter(DriverConfigurationOptions.ActiveContextHandlers.class) Set<EventHandler<ActiveContext>> activeContextEventHandlers,
      final @Parameter(DriverConfigurationOptions.ClosedContextHandlers.class) Set<EventHandler<ClosedContext>> closedContextEventHandlers,
      final @Parameter(DriverConfigurationOptions.FailedContextHandlers.class) Set<EventHandler<FailedContext>> failedContextEventHandlers,
      final @Parameter(DriverConfigurationOptions.ContextMessageHandlers.class) Set<EventHandler<ContextMessage>> contextMessageHandlers,
      final @Parameter(DriverConfigurationOptions.RunningTaskHandlers.class) Set<EventHandler<RunningTask>> runningTaskEventHandlers,
      final @Parameter(DriverConfigurationOptions.CompletedTaskHandlers.class) Set<EventHandler<CompletedTask>> completedTaskEventHandlers,
      final @Parameter(DriverConfigurationOptions.SuspendedTaskHandlers.class) Set<EventHandler<SuspendedTask>> suspendedTaskEventHandlers,
      final @Parameter(DriverConfigurationOptions.TaskMessageHandlers.class) Set<EventHandler<TaskMessage>> taskMessageEventHandlers,
      final @Parameter(DriverConfigurationOptions.FailedTaskHandlers.class) Set<EventHandler<FailedTask>> taskExceptionEventHandlers,
      final @Parameter(DriverConfigurationOptions.AllocatedEvaluatorHandlers.class) Set<EventHandler<AllocatedEvaluator>> allocatedEvaluatorEventHandlers,
      final @Parameter(DriverConfigurationOptions.FailedEvaluatorHandlers.class) Set<EventHandler<FailedEvaluator>> failedEvaluatorHandlers,
      final @Parameter(DriverConfigurationOptions.CompletedEvaluatorHandlers.class) Set<EventHandler<CompletedEvaluator>> completedEvaluatorHandlers,
      final DriverExceptionHandler driverExceptionHandler, final ConfigurationSerializer configurationSerializer) {

    this.clock = clock;
    this.remoteManager = remoteManager;
    this.driverManager = driverManager;
    this.resourceReleaseHandler = resourceReleaseHandler;
    this.resourceLaunchHandler = resourceLaunchHandler;
    this.evaluatorId = evaluatorId;
    this.evaluatorDescriptor = evaluatorDescriptor;
    this.configurationSerializer = configurationSerializer;

    this.dispatcher = new DispatchingEStage(driverExceptionHandler, 16); // 16 threads

    this.dispatcher.register(ActiveContext.class, activeContextEventHandlers);
    this.dispatcher.register(ClosedContext.class, closedContextEventHandlers);
    this.dispatcher.register(FailedContext.class, failedContextEventHandlers);
    this.dispatcher.register(ContextMessage.class, contextMessageHandlers);

    this.dispatcher.register(RunningTask.class, runningTaskEventHandlers);
    this.dispatcher.register(CompletedTask.class, completedTaskEventHandlers);
    this.dispatcher.register(SuspendedTask.class, suspendedTaskEventHandlers);
    this.dispatcher.register(TaskMessage.class, taskMessageEventHandlers);
    this.dispatcher.register(FailedTask.class, taskExceptionEventHandlers);

    this.dispatcher.register(FailedEvaluator.class, failedEvaluatorHandlers);
    this.dispatcher.register(CompletedEvaluator.class, completedEvaluatorHandlers);
    this.dispatcher.register(AllocatedEvaluator.class, allocatedEvaluatorEventHandlers);

    this.dispatcher.onNext(AllocatedEvaluator.class,
        new AllocatedEvaluatorImpl(this, remoteManager.getMyIdentifier(), this.configurationSerializer));
  }

  /**
   * @return NodeDescriptor for the node executing this evaluator
   */
  NodeDescriptor getNodeDescriptor() {
    return this.getEvaluatorDescriptor().getNodeDescriptor();
  }

  /**
   * @return current running task, or null if there is not one.
   */
  RunningTask getRunningTask() {
    synchronized (this.evaluatorDescriptor) {
      return this.runningTask;
    }
  }

  @Override
  public String getId() {
    return this.evaluatorId;
  }

  public void setType(final EvaluatorType type) {
    this.evaluatorDescriptor.setType(type);
  }

  public EvaluatorDescriptor getEvaluatorDescriptor() {
    return this.evaluatorDescriptor;
  }

  @Override
  public void close() {
    synchronized (this.evaluatorDescriptor) {
      if (State.RUNNING == this.state) {
        LOG.log(Level.WARNING, "Dirty shutdown of running evaluator id[{0}]", getId());
        try {
          // Killing the evaluator means that it doesn't need to send a confirmation; it just dies.
          final EvaluatorRuntimeProtocol.EvaluatorControlProto evaluatorControlProto =
              EvaluatorRuntimeProtocol.EvaluatorControlProto.newBuilder()
                  .setTimestamp(System.currentTimeMillis())
                  .setIdentifier(getId())
                  .setKillEvaluator(EvaluatorRuntimeProtocol.KillEvaluatorProto.newBuilder().build())
                  .build();
          handle(evaluatorControlProto);
        } finally {
          this.state = State.KILLED;
        }
      }
    }

    if (!this.isResourceReleased) {
      this.isResourceReleased = true;
      try {
        /* We need to wait awhile before returning the container to the RM in order to
         * give the EvaluatorRuntime (and Launcher) time to cleanly exit. */
        this.clock.scheduleAlarm(100, new EventHandler<Alarm>() {
          @Override
          public void onNext(final Alarm alarm) {
            EvaluatorManager.this.resourceReleaseHandler.onNext(
                DriverRuntimeProtocol.ResourceReleaseProto.newBuilder()
                    .setIdentifier(EvaluatorManager.this.evaluatorId).build()
            );
          }
        });
      } catch (final IllegalStateException e) {
        LOG.log(Level.WARNING, "Force resource release because the client closed the clock.", e);
        EvaluatorManager.this.resourceReleaseHandler.onNext(
            DriverRuntimeProtocol.ResourceReleaseProto.newBuilder()
                .setIdentifier(EvaluatorManager.this.evaluatorId).build()
        );
      } finally {
        EvaluatorManager.this.driverManager.release(EvaluatorManager.this);
      }
    }
  }

  /**
   * Return true if the state is DONE, FAILED, or KILLED,
   * <em>and</em> there are no messages queued or in processing.
   */
  public boolean isClosed() {
    return this.dispatcher.isEmpty() &&
        (this.state == State.DONE || this.state == State.FAILED || this.state == State.KILLED);
  }

  private EvaluatorContext getEvaluatorContext(final String id) {
    for (final EvaluatorContext context : this.activeContextList) {
      if (context.getId().equals(id)) return context;
    }
    throw new RuntimeException("Unknown evaluator context " + id);
  }

  private void addEvaluatorContext(final EvaluatorContext context) {
    this.activeContextList.add(context);
    this.activeContextIds.add(context.getId());
  }

  private void removeEvaluatorContext(final EvaluatorContext context) {
    this.activeContextList.remove(context);
    this.activeContextIds.remove(context.getId());
  }

  /**
   * EvaluatorException will trigger is FailedEvaluator and state transition to FAILED
   *
   * @param exception on the EvaluatorRuntime
   */
  void handle(final EvaluatorException exception) {
    synchronized (this.evaluatorDescriptor) {
      if (this.state.ordinal() >= State.DONE.ordinal()) return;

      LOG.log(Level.WARNING, "Failed evaluator: " + getId(), exception);

      try {

        final List<FailedContext> failedContextList = new ArrayList<>();
        final List<EvaluatorContext> activeContexts = new ArrayList<>(this.activeContextList);
        Collections.reverse(activeContexts);

        for (final EvaluatorContext context : activeContexts) {
          final Optional<ActiveContext> parentContext = context.getParentId().isPresent() ?
              Optional.<ActiveContext>of(getEvaluatorContext(context.getParentId().get())) :
              Optional.<ActiveContext>empty();
          failedContextList.add(context.getFailedContext(parentContext, exception));
        }

        final Optional<FailedTask> failedTaskOptional = this.runningTask != null ?
            Optional.of(new FailedTask(this.runningTask.getId(), exception)) :
            Optional.<FailedTask>empty();

        this.dispatcher.onNext(FailedEvaluator.class, new FailedEvaluatorImpl(
            exception, failedContextList, failedTaskOptional, this.evaluatorId));

      } catch (final Exception e) {
        LOG.log(Level.SEVERE, "Exception while handling FailedEvaluator", e);
      } finally {
        this.state = State.FAILED;
        close();
      }
    }
  }

  void handle(final RemoteMessage<EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto> evaluatorHeartbeatProtoRemoteMessage) {
    synchronized (this.evaluatorDescriptor) {
      final EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto evaluatorHeartbeatProto = evaluatorHeartbeatProtoRemoteMessage.getMessage();

      if (evaluatorHeartbeatProto.hasEvaluatorStatus()) {
        final ReefServiceProtos.EvaluatorStatusProto status = evaluatorHeartbeatProto.getEvaluatorStatus();
        if (status.hasError()) {
          final ObjectSerializableCodec<Exception> codec = new ObjectSerializableCodec<>();
          handle(new EvaluatorException(getId(), codec.decode(status.getError().toByteArray())));
          return;
        } else if (State.SUBMITTED == this.state) {
          final String evaluatorRID = evaluatorHeartbeatProtoRemoteMessage.getIdentifier().toString();
          this.evaluatorControlHandler = remoteManager.getHandler(evaluatorRID, EvaluatorRuntimeProtocol.EvaluatorControlProto.class);
          this.state = State.RUNNING;
          LOG.log(Level.FINEST, "Evaluator {0} is running", this.evaluatorId);
        }
      }

      LOG.log(Level.FINEST, "Evaluator heartbeat: {0}", evaluatorHeartbeatProto);

      final ReefServiceProtos.EvaluatorStatusProto evaluatorStatusProto = evaluatorHeartbeatProto.getEvaluatorStatus();

      for (final ReefServiceProtos.ContextStatusProto contextStatusProto : evaluatorHeartbeatProto.getContextStatusList()) {
        handle(contextStatusProto, !evaluatorHeartbeatProto.hasTaskStatus());
      }

      if (evaluatorHeartbeatProto.hasTaskStatus()) {
        handle(evaluatorHeartbeatProto.getTaskStatus());
      }

      if (ReefServiceProtos.State.FAILED == evaluatorStatusProto.getState()) {
        this.state = State.FAILED;
        final ObjectSerializableCodec<Exception> codec = new ObjectSerializableCodec<>();
        final EvaluatorException evaluatorException = evaluatorStatusProto.hasError() ?
            new EvaluatorException(this.evaluatorId, codec.decode(evaluatorStatusProto.getError().toByteArray()), this.runningTask) :
            new EvaluatorException(this.evaluatorId, "unknown cause");
        LOG.log(Level.WARNING, "Failed evaluator: " + getId(), evaluatorException);
        this.handle(evaluatorException);
      } else if (ReefServiceProtos.State.DONE == evaluatorStatusProto.getState()) {
        LOG.log(Level.FINEST, "Evaluator {0} done.", getId());
        this.state = State.DONE;

        dispatcher.onNext(CompletedEvaluator.class, new CompletedEvaluatorImpl(this.evaluatorId));

        close();
      }

      LOG.info("DONE with evaluator heartbeat");
    }
  }

  void handle(final DriverRuntimeProtocol.ResourceLaunchProto resourceLaunchProto) {
    synchronized (this.evaluatorDescriptor) {
      if (State.ALLOCATED == this.state) {
        this.state = State.SUBMITTED;
        this.resourceLaunchHandler.onNext(resourceLaunchProto);
      } else {
        throw new RuntimeException("Evaluator manager expected " + State.ALLOCATED +
            " state but instead is in state " + this.state);
      }
    }
  }

  /**
   * Packages the TaskControlProto in an EvaluatorControlProto and forward it to the EvaluatorRuntime
   *
   * @param taskControlProto message contains task control info.
   */
  void handle(final EvaluatorRuntimeProtocol.ContextControlProto taskControlProto) {
    synchronized (this.evaluatorDescriptor) {
      LOG.log(Level.FINEST, "Task control message from {0}", this.evaluatorId);

      final EvaluatorRuntimeProtocol.EvaluatorControlProto evaluatorControlProto =
          EvaluatorRuntimeProtocol.EvaluatorControlProto.newBuilder()
              .setTimestamp(System.currentTimeMillis())
              .setIdentifier(getId())
              .setContextControl(taskControlProto).build();
      handle(evaluatorControlProto);
    }
  }

  /**
   * Forward the EvaluatorControlProto to the EvaluatorRuntime
   *
   * @param evaluatorControlProto message contains evaluator control information.
   */
  void handle(final EvaluatorRuntimeProtocol.EvaluatorControlProto evaluatorControlProto) {
    synchronized (this.evaluatorDescriptor) {
      if (State.RUNNING == this.state) {
        this.evaluatorControlHandler.onNext(evaluatorControlProto);
      } else {
        throw new RuntimeException("Evaluator manager expects to be in " +
            State.RUNNING + " state, but instead is in state " + this.state);
      }
    }
  }

  /**
   * Handle a context status update
   *
   * @param contextStatusProto indicating the current status of the context
   */
  private void handle(final ReefServiceProtos.ContextStatusProto contextStatusProto,
                      final boolean notifyClientOnNewActiveContext) {

    final String contextID = contextStatusProto.getContextId();
    final Optional<String> parentID = contextStatusProto.hasParentId() ?
        Optional.of(contextStatusProto.getParentId()) : Optional.<String>empty();

    if (ReefServiceProtos.ContextStatusProto.State.READY == contextStatusProto.getContextState()) {
      if (!this.activeContextIds.contains(contextID)) {
        final EvaluatorContext context = new EvaluatorContext(this, contextID, parentID, configurationSerializer);
        addEvaluatorContext(context);
        if (notifyClientOnNewActiveContext) {
          this.dispatcher.onNext(ActiveContext.class, context);
        }
      }

      for (final ReefServiceProtos.ContextStatusProto.ContextMessageProto contextMessageProto : contextStatusProto.getContextMessageList()) {
        final byte[] theMessage = contextMessageProto.getMessage().toByteArray();
        final String sourceID = contextMessageProto.getSourceId();
        this.dispatcher.onNext(ContextMessage.class,
            new ContextMessageImpl(theMessage, contextID, sourceID));
      }
    } else {
      if (!this.activeContextIds.contains(contextID)) {
        if (ReefServiceProtos.ContextStatusProto.State.FAIL == contextStatusProto.getContextState()) {
          // It failed right away
          addEvaluatorContext(new EvaluatorContext(this, contextID, parentID, configurationSerializer));
        } else {
          throw new RuntimeException("unknown context signaling state " + contextStatusProto.getContextState());
        }
      }

      final EvaluatorContext context = getEvaluatorContext(contextID);
      final EvaluatorContext parentContext = context.getParentId().isPresent() ?
          getEvaluatorContext(context.getParentId().get()) : null;
      removeEvaluatorContext(context);

      if (ReefServiceProtos.ContextStatusProto.State.FAIL == contextStatusProto.getContextState()) {
        final ObjectSerializableCodec<Exception> codec = new ObjectSerializableCodec<>();
        final Exception reason = codec.decode(contextStatusProto.getError().toByteArray());
        final Optional<ActiveContext> optionalParentContext = (null == parentContext) ?
            Optional.<ActiveContext>empty() : Optional.<ActiveContext>of(parentContext);
        this.dispatcher.onNext(FailedContext.class,
            context.getFailedContext(optionalParentContext, reason));
      } else if (ReefServiceProtos.ContextStatusProto.State.DONE == contextStatusProto.getContextState()) {
        if (null != parentContext) {
          this.dispatcher.onNext(ClosedContext.class, context.getClosedContext(parentContext));
        } else {
          LOG.info("Root context closed. Evaluator closed will trigger final shutdown.");
        }
      } else {
        throw new RuntimeException("Unknown context state " + contextStatusProto.getContextState() +
            " for context " + contextID);
      }
    }
  }

  /**
   * Handle task status messages.
   *
   * @param taskStatusProto message contains the current task status.
   */
  private void handle(final ReefServiceProtos.TaskStatusProto taskStatusProto) {

    LOG.log(Level.FINEST, "Received task {0} status {1}",
        new Object[]{taskStatusProto.getTaskId(), taskStatusProto.getState()});

    final String taskId = taskStatusProto.getTaskId();
    final String contextId = taskStatusProto.getContextId();
    final ReefServiceProtos.State taskState = taskStatusProto.getState();

    if (ReefServiceProtos.State.INIT == taskState) {
      final EvaluatorContext evaluatorContext = getEvaluatorContext(contextId);
      this.runningTask = new RunningTaskImpl(this, taskId, evaluatorContext);
      this.dispatcher.onNext(RunningTask.class, this.runningTask);
    } else if (ReefServiceProtos.State.SUSPEND == taskState) {
      final EvaluatorContext evaluatorContext = getEvaluatorContext(contextId);
      this.runningTask = null;
      final byte[] message = taskStatusProto.hasResult() ? taskStatusProto.getResult().toByteArray() : null;
      this.dispatcher.onNext(SuspendedTask.class,
          new SuspendedTaskImpl(evaluatorContext, message, taskId));
    } else if (ReefServiceProtos.State.DONE == taskState) {
      final EvaluatorContext evaluatorContext = getEvaluatorContext(contextId);
      this.runningTask = null;
      final byte[] message = taskStatusProto.hasResult() ? taskStatusProto.getResult().toByteArray() : null;
      this.dispatcher.onNext(CompletedTask.class,
          new CompletedTaskImpl(evaluatorContext, message, taskId));
    } else if (ReefServiceProtos.State.FAILED == taskState) {
      this.runningTask = null;
      final ObjectSerializableCodec<Exception> codec = new ObjectSerializableCodec<>();
      /* Assuming nothing went wrong with the context:
       * The failed task could have corrupted it, but I can't make this call.  */
      final EvaluatorContext evaluatorContext = getEvaluatorContext(contextId);
      final FailedTask taskException = taskStatusProto.hasResult() ?
          new FailedTask(taskId, codec.decode(taskStatusProto.getResult().toByteArray()), Optional.<ActiveContext>of(evaluatorContext)) :
          new FailedTask(taskId, "Failed Task: " + taskState, Optional.<ActiveContext>of(evaluatorContext));

      this.dispatcher.onNext(FailedTask.class, taskException);
    } else if (taskStatusProto.getTaskMessageCount() > 0) {
      assert (this.runningTask != null);
      for (final ReefServiceProtos.TaskStatusProto.TaskMessageProto taskMessageProto : taskStatusProto.getTaskMessageList()) {
        this.dispatcher.onNext(TaskMessage.class,
            new TaskMessageImpl(taskMessageProto.getMessage().toByteArray(),
                taskId, contextId, taskMessageProto.getSourceId())
        );
      }
    }
  }

  /**
   * Resource status information from the (actual) resource manager.
   */
  void handle(final DriverRuntimeProtocol.ResourceStatusProto resourceStatusProto) {
    synchronized (this.evaluatorDescriptor) {
      LOG.log(Level.FINEST, "Resource manager state update: {0}", resourceStatusProto.getState());

      if (resourceStatusProto.getState() == ReefServiceProtos.State.DONE ||
          resourceStatusProto.getState() == ReefServiceProtos.State.FAILED) {

        if (this.state.ordinal() < State.DONE.ordinal()) {

          // something is wrong, I think I'm alive but the resource manager runtime says I'm dead
          final StringBuilder sb = new StringBuilder();
          sb.append("The resource manager informed me that Evaluator " + this.evaluatorId +
              " is in state " + resourceStatusProto.getState() + " but I think I'm in state " + this.state);
          if (resourceStatusProto.getDiagnostics() != null && "".equals(resourceStatusProto.getDiagnostics())) {
            sb.append("Cause: " + resourceStatusProto.getDiagnostics());
          }

          if (runningTask != null) {
            sb.append("TaskRuntime " + runningTask.getId() + " did not complete before this evaluator died. ");
          }

          // RM is telling me its DONE/FAILED - assuming it has already released the resources
          this.isResourceReleased = true;
          handle(new EvaluatorException(this.evaluatorId, sb.toString(), runningTask));
          this.state = State.KILLED;
        }
      }
    }
  }
}
