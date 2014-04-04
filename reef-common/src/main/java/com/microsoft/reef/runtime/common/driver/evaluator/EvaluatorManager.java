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
package com.microsoft.reef.runtime.common.driver.evaluator;

import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.driver.catalog.NodeDescriptor;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.FailedContext;
import com.microsoft.reef.driver.evaluator.EvaluatorDescriptor;
import com.microsoft.reef.driver.evaluator.EvaluatorType;
import com.microsoft.reef.driver.task.FailedTask;
import com.microsoft.reef.driver.task.RunningTask;
import com.microsoft.reef.exception.EvaluatorException;
import com.microsoft.reef.io.naming.Identifiable;
import com.microsoft.reef.proto.DriverRuntimeProtocol;
import com.microsoft.reef.proto.EvaluatorRuntimeProtocol;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.runtime.common.driver.DriverExceptionHandler;
import com.microsoft.reef.runtime.common.driver.DriverManager;
import com.microsoft.reef.runtime.common.driver.api.ResourceLaunchHandler;
import com.microsoft.reef.runtime.common.driver.api.ResourceReleaseHandler;
import com.microsoft.reef.runtime.common.driver.context.ContextControlHandler;
import com.microsoft.reef.runtime.common.driver.context.ContextMessageImpl;
import com.microsoft.reef.runtime.common.driver.context.EvaluatorContext;
import com.microsoft.reef.runtime.common.driver.task.CompletedTaskImpl;
import com.microsoft.reef.runtime.common.driver.task.RunningTaskImpl;
import com.microsoft.reef.runtime.common.driver.task.SuspendedTaskImpl;
import com.microsoft.reef.runtime.common.driver.task.TaskMessageImpl;
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

  private final static Logger LOG = Logger.getLogger(EvaluatorManager.class.getName());
  private final Clock clock;
  private final DriverManager driverManager;
  private final ResourceReleaseHandler resourceReleaseHandler;
  private final ResourceLaunchHandler resourceLaunchHandler;
  private final String evaluatorId;
  private final EvaluatorDescriptorImpl evaluatorDescriptor;
  private final List<EvaluatorContext> activeContextList = new ArrayList<>();
  private final Set<String> activeContextIds = new HashSet<>();
  private final EvaluatorMessageDispatcher messageDispatcher;
  private final ConfigurationSerializer configurationSerializer;
  private final EvaluatorControlHandler evaluatorControlHandler;
  private final ContextControlHandler contextControlHandler;
  private final EvaluatorStateManager stateManager;
  private RunningTask runningTask = null;

  private boolean isResourceReleased = false;

  // Mutable fields

  @Inject
  EvaluatorManager(
      final Clock clock,
      final RemoteManager remoteManager,
      final DriverManager driverManager,
      final ResourceReleaseHandler resourceReleaseHandler,
      final ResourceLaunchHandler resourceLaunchHandler,
      final @Parameter(EvaluatorIdentifier.class) String evaluatorId,
      final @Parameter(EvaluatorDescriptorName.class) EvaluatorDescriptorImpl evaluatorDescriptor,
      final DriverExceptionHandler driverExceptionHandler,
      final ConfigurationSerializer configurationSerializer,
      final EvaluatorMessageDispatcher messageDispatcher,
      final EvaluatorControlHandler evaluatorControlHandler,
      final ContextControlHandler contextControlHandler,
      final EvaluatorStateManager stateManager) {

    this.clock = clock;
    this.driverManager = driverManager;
    this.resourceReleaseHandler = resourceReleaseHandler;
    this.resourceLaunchHandler = resourceLaunchHandler;
    this.evaluatorId = evaluatorId;
    this.evaluatorDescriptor = evaluatorDescriptor;
    this.configurationSerializer = configurationSerializer;

    this.messageDispatcher = messageDispatcher;
    this.evaluatorControlHandler = evaluatorControlHandler;
    this.contextControlHandler = contextControlHandler;
    this.stateManager = stateManager;

    this.messageDispatcher.onEvaluatorAllocated(new AllocatedEvaluatorImpl(this, remoteManager.getMyIdentifier(), this.configurationSerializer));
  }

  /**
   * @return NodeDescriptor for the node executing this evaluator
   */
  public NodeDescriptor getNodeDescriptor() {
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
      if (this.stateManager.isRunning()) {
        LOG.log(Level.WARNING, "Dirty shutdown of running evaluator id[{0}]", getId());
        try {
          // Killing the evaluator means that it doesn't need to send a confirmation; it just dies.
          final EvaluatorRuntimeProtocol.EvaluatorControlProto evaluatorControlProto =
              EvaluatorRuntimeProtocol.EvaluatorControlProto.newBuilder()
                  .setTimestamp(System.currentTimeMillis())
                  .setIdentifier(getId())
                  .setKillEvaluator(EvaluatorRuntimeProtocol.KillEvaluatorProto.newBuilder().build())
                  .build();
          sendEvaluatorControlMessage(evaluatorControlProto);
        } finally {
          this.stateManager.setKilled();
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
    return this.messageDispatcher.isEmpty() &&
        (this.stateManager.isDoneOrFailedOrKilled());
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
  public void onEvaluatorException(final EvaluatorException exception) {
    synchronized (this.evaluatorDescriptor) {
      if (this.stateManager.isDoneOrFailedOrKilled()) {
        return;
      }

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

        this.messageDispatcher.onEvaluatorFailed(new FailedEvaluatorImpl(exception, failedContextList, failedTaskOptional, this.evaluatorId));

      } catch (final Exception e) {
        LOG.log(Level.SEVERE, "Exception while handling FailedEvaluator", e);
      } finally {
        this.stateManager.setFailed();
        close();
      }
    }
  }

  public void onEvaluatorHeartbeatMessage(final RemoteMessage<EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto> evaluatorHeartbeatProtoRemoteMessage) {
    synchronized (this.evaluatorDescriptor) {
      final EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto evaluatorHeartbeatProto = evaluatorHeartbeatProtoRemoteMessage.getMessage();

      if (evaluatorHeartbeatProto.hasEvaluatorStatus()) {
        final ReefServiceProtos.EvaluatorStatusProto status = evaluatorHeartbeatProto.getEvaluatorStatus();
        if (status.hasError()) {
          final ObjectSerializableCodec<Exception> codec = new ObjectSerializableCodec<>();
          onEvaluatorException(new EvaluatorException(getId(), codec.decode(status.getError().toByteArray())));
          return;
        } else if (this.stateManager.isSubmitted()) {
          final String evaluatorRID = evaluatorHeartbeatProtoRemoteMessage.getIdentifier().toString();
          this.evaluatorControlHandler.setRemoteID(evaluatorRID);
          this.stateManager.setRunning();
          LOG.log(Level.FINEST, "Evaluator {0} is running", this.evaluatorId);
        }
      }

      LOG.log(Level.FINEST, "Evaluator heartbeat: {0}", evaluatorHeartbeatProto);

      final ReefServiceProtos.EvaluatorStatusProto evaluatorStatusProto = evaluatorHeartbeatProto.getEvaluatorStatus();

      for (final ReefServiceProtos.ContextStatusProto contextStatusProto : evaluatorHeartbeatProto.getContextStatusList()) {
        onContextStatusMessage(contextStatusProto, !evaluatorHeartbeatProto.hasTaskStatus());
      }

      if (evaluatorHeartbeatProto.hasTaskStatus()) {
        onTaskStatusMessage(evaluatorHeartbeatProto.getTaskStatus());
      }

      if (ReefServiceProtos.State.FAILED == evaluatorStatusProto.getState()) {
        this.stateManager.setFailed();
        final ObjectSerializableCodec<Exception> codec = new ObjectSerializableCodec<>();
        final EvaluatorException evaluatorException = evaluatorStatusProto.hasError() ?
            new EvaluatorException(this.evaluatorId, codec.decode(evaluatorStatusProto.getError().toByteArray()), this.runningTask) :
            new EvaluatorException(this.evaluatorId, "unknown cause");
        LOG.log(Level.WARNING, "Failed evaluator: " + getId(), evaluatorException);
        this.onEvaluatorException(evaluatorException);
      } else if (ReefServiceProtos.State.DONE == evaluatorStatusProto.getState()) {
        LOG.log(Level.FINEST, "Evaluator {0} done.", getId());
        this.stateManager.setDone();

        this.messageDispatcher.onEvaluatorCompleted(new CompletedEvaluatorImpl(this.evaluatorId));
        close();
      }

      LOG.info("DONE with evaluator heartbeat");
    }
  }

  public void onResourceLaunch(final DriverRuntimeProtocol.ResourceLaunchProto resourceLaunchProto) {
    synchronized (this.evaluatorDescriptor) {
      if (this.stateManager.isAllocated()) {
        this.stateManager.setSubmitted();
        this.resourceLaunchHandler.onNext(resourceLaunchProto);
      } else {
        throw new RuntimeException("Evaluator manager expected " + State.ALLOCATED +
            " state but instead is in state " + this.stateManager);
      }
    }
  }

  /**
   * Packages the ContextControlProto in an EvaluatorControlProto and forward it to the EvaluatorRuntime
   *
   * @param contextControlProto message contains context control info.
   */
  public void sendContextControlMessage(final EvaluatorRuntimeProtocol.ContextControlProto contextControlProto) {
    synchronized (this.evaluatorDescriptor) {
      LOG.log(Level.FINEST, "Context control message to {0}", this.evaluatorId);
      this.contextControlHandler.send(contextControlProto);
    }
  }

  /**
   * Forward the EvaluatorControlProto to the EvaluatorRuntime
   *
   * @param evaluatorControlProto message contains evaluator control information.
   */
  public void sendEvaluatorControlMessage(final EvaluatorRuntimeProtocol.EvaluatorControlProto evaluatorControlProto) {
    synchronized (this.evaluatorDescriptor) {
      this.evaluatorControlHandler.send(evaluatorControlProto);
    }
  }

  /**
   * Handle a context status update
   *
   * @param contextStatusProto indicating the current status of the context
   */

  private void onContextStatusMessage(final ReefServiceProtos.ContextStatusProto contextStatusProto,
                                      final boolean notifyClientOnNewActiveContext) {

    final String contextID = contextStatusProto.getContextId();
    final Optional<String> parentID = contextStatusProto.hasParentId() ?
        Optional.of(contextStatusProto.getParentId()) : Optional.<String>empty();

    if (ReefServiceProtos.ContextStatusProto.State.READY == contextStatusProto.getContextState()) {
      if (!this.activeContextIds.contains(contextID)) {
        final EvaluatorContext context = new EvaluatorContext(contextID, evaluatorId, this.evaluatorDescriptor, parentID, configurationSerializer, contextControlHandler);
        addEvaluatorContext(context);
        if (notifyClientOnNewActiveContext) {
          this.messageDispatcher.onContextActive(context);
        }
      }

      for (final ReefServiceProtos.ContextStatusProto.ContextMessageProto contextMessageProto : contextStatusProto.getContextMessageList()) {
        final byte[] theMessage = contextMessageProto.getMessage().toByteArray();
        final String sourceID = contextMessageProto.getSourceId();
        this.messageDispatcher.onContextMessage(new ContextMessageImpl(theMessage, contextID, sourceID));
      }
    } else {
      if (!this.activeContextIds.contains(contextID)) {
        if (ReefServiceProtos.ContextStatusProto.State.FAIL == contextStatusProto.getContextState()) {
          // It failed right away
          addEvaluatorContext(new EvaluatorContext(contextID, this.evaluatorId, this.evaluatorDescriptor, parentID, configurationSerializer, contextControlHandler));
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
        this.messageDispatcher.onContextFailed(context.getFailedContext(optionalParentContext, reason));
      } else if (ReefServiceProtos.ContextStatusProto.State.DONE == contextStatusProto.getContextState()) {
        if (null != parentContext) {
          this.messageDispatcher.onContextClose(context.getClosedContext(parentContext));
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
  private void onTaskStatusMessage(final ReefServiceProtos.TaskStatusProto taskStatusProto) {

    LOG.log(Level.FINEST, "Received task {0} status {1}",
        new Object[]{taskStatusProto.getTaskId(), taskStatusProto.getState()});

    final String taskId = taskStatusProto.getTaskId();
    final String contextId = taskStatusProto.getContextId();
    final ReefServiceProtos.State taskState = taskStatusProto.getState();

    if (ReefServiceProtos.State.INIT == taskState) {
      final EvaluatorContext evaluatorContext = getEvaluatorContext(contextId);
      this.runningTask = new RunningTaskImpl(this, taskId, evaluatorContext);
      this.messageDispatcher.onTaskRunning(this.runningTask);
    } else if (ReefServiceProtos.State.SUSPEND == taskState) {
      final EvaluatorContext evaluatorContext = getEvaluatorContext(contextId);
      this.runningTask = null;
      final byte[] message = taskStatusProto.hasResult() ? taskStatusProto.getResult().toByteArray() : null;
      this.messageDispatcher.onTaskSuspended(new SuspendedTaskImpl(evaluatorContext, message, taskId));
    } else if (ReefServiceProtos.State.DONE == taskState) {
      final EvaluatorContext evaluatorContext = getEvaluatorContext(contextId);
      this.runningTask = null;
      final byte[] message = taskStatusProto.hasResult() ? taskStatusProto.getResult().toByteArray() : null;
      this.messageDispatcher.onTaskCompleted(new CompletedTaskImpl(evaluatorContext, message, taskId));
    } else if (ReefServiceProtos.State.FAILED == taskState) {
      this.runningTask = null;
      final ObjectSerializableCodec<Exception> codec = new ObjectSerializableCodec<>();
      /* Assuming nothing went wrong with the context:
       * The failed task could have corrupted it, but I can't make this call.  */
      final EvaluatorContext evaluatorContext = getEvaluatorContext(contextId);
      final FailedTask taskException = taskStatusProto.hasResult() ?
          new FailedTask(taskId, codec.decode(taskStatusProto.getResult().toByteArray()), Optional.<ActiveContext>of(evaluatorContext)) :
          new FailedTask(taskId, "Failed Task: " + taskState, Optional.<ActiveContext>of(evaluatorContext));

      this.messageDispatcher.onTaskFailed(taskException);
    } else if (taskStatusProto.getTaskMessageCount() > 0) {
      assert (this.runningTask != null);
      for (final ReefServiceProtos.TaskStatusProto.TaskMessageProto taskMessageProto : taskStatusProto.getTaskMessageList()) {
        this.messageDispatcher.onTaskMessage(
            new TaskMessageImpl(taskMessageProto.getMessage().toByteArray(),
                taskId, contextId, taskMessageProto.getSourceId())
        );
      }
    }
  }

  /**
   * Resource status information from the (actual) resource manager.
   */
  public void onResourceStatusMessage(final DriverRuntimeProtocol.ResourceStatusProto resourceStatusProto) {
    synchronized (this.evaluatorDescriptor) {
      LOG.log(Level.FINEST, "Resource manager state update: {0}", resourceStatusProto.getState());

      if (resourceStatusProto.getState() == ReefServiceProtos.State.DONE ||
          resourceStatusProto.getState() == ReefServiceProtos.State.FAILED) {

        if (this.stateManager.isAllocatedOrSubmittedOrRunning()) {

          // something is wrong, I think I'm alive but the resource manager runtime says I'm dead
          final StringBuilder sb = new StringBuilder();
          sb.append("The resource manager informed me that Evaluator " + this.evaluatorId +
              " is in state " + resourceStatusProto.getState() + " but I think I'm in state " + this.stateManager);
          if (resourceStatusProto.getDiagnostics() != null && "".equals(resourceStatusProto.getDiagnostics())) {
            sb.append("Cause: " + resourceStatusProto.getDiagnostics());
          }

          if (runningTask != null) {
            sb.append("TaskRuntime " + runningTask.getId() + " did not complete before this evaluator died. ");
          }

          // RM is telling me its DONE/FAILED - assuming it has already released the resources
          this.isResourceReleased = true;
          onEvaluatorException(new EvaluatorException(this.evaluatorId, sb.toString(), runningTask));
          this.stateManager.setKilled();
        }
      }
    }
  }

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

  // Dynamic Parameters
  @NamedParameter(doc = "The Evaluator Identifier.")
  public final static class EvaluatorIdentifier implements Name<String> {
  }

  @NamedParameter(doc = "The Evaluator Host.")
  public final static class EvaluatorDescriptorName implements Name<EvaluatorDescriptorImpl> {
  }
}
