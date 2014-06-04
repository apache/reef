/**
 * Copyright (C) 2014 Microsoft Corporation
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

import com.microsoft.reef.annotations.audience.DriverSide;
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
import com.microsoft.reef.runtime.common.driver.api.ResourceLaunchHandler;
import com.microsoft.reef.runtime.common.driver.api.ResourceReleaseHandler;
import com.microsoft.reef.runtime.common.driver.context.ContextControlHandler;
import com.microsoft.reef.runtime.common.driver.context.ContextMessageImpl;
import com.microsoft.reef.runtime.common.driver.context.EvaluatorContext;
import com.microsoft.reef.runtime.common.driver.task.CompletedTaskImpl;
import com.microsoft.reef.runtime.common.driver.task.RunningTaskImpl;
import com.microsoft.reef.runtime.common.driver.task.SuspendedTaskImpl;
import com.microsoft.reef.runtime.common.driver.task.TaskMessageImpl;
import com.microsoft.reef.runtime.common.utils.ExceptionCodec;
import com.microsoft.reef.runtime.common.utils.RemoteManager;
import com.microsoft.reef.util.Optional;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.formats.ConfigurationSerializer;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.RemoteMessage;
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
@DriverSide
public final class EvaluatorManager implements Identifiable, AutoCloseable {

  private final static Logger LOG = Logger.getLogger(EvaluatorManager.class.getName());

  private final EvaluatorHeartBeatSanityChecker sanityChecker = new EvaluatorHeartBeatSanityChecker();
  private final Clock clock;
  private final Evaluators evaluators;
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
  private final EvaluatorStatusManager stateManager;
  private final ExceptionCodec exceptionCodec;


  // Mutable fields
  private RunningTask runningTask = null;
  private boolean isResourceReleased = false;

  @Inject
  EvaluatorManager(
      final Clock clock,
      final RemoteManager remoteManager,
      final Evaluators evaluators,
      final ResourceReleaseHandler resourceReleaseHandler,
      final ResourceLaunchHandler resourceLaunchHandler,
      final @Parameter(EvaluatorIdentifier.class) String evaluatorId,
      final @Parameter(EvaluatorDescriptorName.class) EvaluatorDescriptorImpl evaluatorDescriptor,
      final DriverExceptionHandler driverExceptionHandler,
      final ConfigurationSerializer configurationSerializer,
      final EvaluatorMessageDispatcher messageDispatcher,
      final EvaluatorControlHandler evaluatorControlHandler,
      final ContextControlHandler contextControlHandler,
      final EvaluatorStatusManager stateManager, ExceptionCodec exceptionCodec) {

    this.clock = clock;
    this.evaluators = evaluators;
    this.resourceReleaseHandler = resourceReleaseHandler;
    this.resourceLaunchHandler = resourceLaunchHandler;
    this.evaluatorId = evaluatorId;
    this.evaluatorDescriptor = evaluatorDescriptor;
    this.configurationSerializer = configurationSerializer;

    this.messageDispatcher = messageDispatcher;
    this.evaluatorControlHandler = evaluatorControlHandler;
    this.contextControlHandler = contextControlHandler;
    this.stateManager = stateManager;
    this.exceptionCodec = exceptionCodec;


    this.messageDispatcher.onEvaluatorAllocated(new AllocatedEvaluatorImpl(
        this, remoteManager.getMyIdentifier(), this.configurationSerializer));
    LOG.log(Level.INFO, "Instantiated 'EvaluatorManager' for evaluator: {0}", this.getId());
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
          EvaluatorManager.this.evaluators.remove(EvaluatorManager.this);
        }
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

  public EvaluatorContext getEvaluatorContext(final String id) {
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
          failedContextList.add(context.getFailedContextForEvaluatorFailure());
        }

        final Optional<FailedTask> failedTaskOptional;
        if (null != this.runningTask) {
          final String taskId = this.runningTask.getId();
          final Optional<ActiveContext> evaluatorContext = Optional.<ActiveContext>empty();
          final Optional<byte[]> bytes = Optional.<byte[]>empty();
          final Optional<Throwable> taskException = Optional.<Throwable>of(new Exception("Evaluator crash"));
          final String message = "Evaluator crash";
          final Optional<String> description = Optional.empty();
          final FailedTask failedTask = new FailedTask(taskId, message, description, taskException, bytes, evaluatorContext);
          failedTaskOptional = Optional.of(failedTask);
        } else {
          failedTaskOptional = Optional.empty();
        }


        this.messageDispatcher.onEvaluatorFailed(new FailedEvaluatorImpl(exception, failedContextList, failedTaskOptional, this.evaluatorId));

      } catch (final Exception e) {
        LOG.log(Level.SEVERE, "Exception while handling FailedEvaluator", e);
      } finally {
        this.stateManager.setFailed();
        close();
      }
    }
  }

  public synchronized void onEvaluatorHeartbeatMessage(
      final RemoteMessage<EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto> evaluatorHeartbeatProtoRemoteMessage) {

    final EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto evaluatorHeartbeatProto =
        evaluatorHeartbeatProtoRemoteMessage.getMessage();

    LOG.log(Level.FINEST, "Evaluator heartbeat: {0}", evaluatorHeartbeatProto);

    this.sanityChecker.check(evaluatorId, evaluatorHeartbeatProto.getTimestamp());

    // If this is the first message from this Evaluator, register it.
    if (this.stateManager.isSubmitted()) {
      final String evaluatorRID = evaluatorHeartbeatProtoRemoteMessage.getIdentifier().toString();
      this.evaluatorControlHandler.setRemoteID(evaluatorRID);
      this.stateManager.setRunning();
      LOG.log(Level.FINEST, "Evaluator {0} is running", this.evaluatorId);
    }

    // Process the Evaluator status message
    if (evaluatorHeartbeatProto.hasEvaluatorStatus()) {
      this.onEvaluatorStatusMessage(evaluatorHeartbeatProto.getEvaluatorStatus());
    }

    // Process the Context status message(s)
    for (final ReefServiceProtos.ContextStatusProto contextStatusProto : evaluatorHeartbeatProto.getContextStatusList()) {
      this.onContextStatusMessage(contextStatusProto, !evaluatorHeartbeatProto.hasTaskStatus());
    }

    // Process the Task status message
    if (evaluatorHeartbeatProto.hasTaskStatus()) {
      this.onTaskStatusMessage(evaluatorHeartbeatProto.getTaskStatus());
    }
    LOG.log(Level.FINE, "DONE with evaluator heartbeat from Evaluator {0}", this.getId());
  }

  /**
   * Process a evaluator status message.
   *
   * @param message
   */
  private synchronized void onEvaluatorStatusMessage(final ReefServiceProtos.EvaluatorStatusProto message) {

    switch (message.getState()) {
      case DONE:
        this.onEvaluatorDone(message);
        break;
      case FAILED:
        this.onEvaluatorFailed(message);
        break;
      case INIT:
      case KILLED:
      case RUNNING:
      case SUSPEND:
        break;
    }

    if (message.getState() == ReefServiceProtos.State.FAILED) {
      this.onEvaluatorFailed(message);
    }
  }

  /**
   * Process an evaluator message that indicates that the evaluator shut down cleanly.
   *
   * @param message
   */
  private synchronized void onEvaluatorDone(final ReefServiceProtos.EvaluatorStatusProto message) {
    assert (message.getState() == ReefServiceProtos.State.DONE);
    LOG.log(Level.FINEST, "Evaluator {0} done.", getId());
    this.stateManager.setDone();
    this.messageDispatcher.onEvaluatorCompleted(new CompletedEvaluatorImpl(this.evaluatorId));
    close();
  }

  /**
   * Process an evaluator message that indicates a crash.
   *
   * @param evaluatorStatusProto
   */
  private synchronized void onEvaluatorFailed(final ReefServiceProtos.EvaluatorStatusProto evaluatorStatusProto) {
    assert (evaluatorStatusProto.getState() == ReefServiceProtos.State.FAILED);
    final EvaluatorException evaluatorException;
    if (evaluatorStatusProto.hasError()) {
      final Optional<Throwable> exception = this.exceptionCodec.fromBytes(evaluatorStatusProto.getError().toByteArray());
      if (exception.isPresent()) {
        evaluatorException = new EvaluatorException(getId(), exception.get());
      } else {
        evaluatorException = new EvaluatorException(getId(), new Exception("Exception sent, but can't be deserialized"));
      }
    } else {
      evaluatorException = new EvaluatorException(getId(), new Exception("No exception sent"));
    }
    onEvaluatorException(evaluatorException);
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
  private synchronized void onContextStatusMessage(final ReefServiceProtos.ContextStatusProto contextStatusProto,
                                                   final boolean notifyClientOnNewActiveContext) {

    final String contextID = contextStatusProto.getContextId();
    final Optional<String> parentID = contextStatusProto.hasParentId() ?
        Optional.of(contextStatusProto.getParentId()) : Optional.<String>empty();

    if (ReefServiceProtos.ContextStatusProto.State.READY == contextStatusProto.getContextState()) {
      if (!this.activeContextIds.contains(contextID)) {
        final EvaluatorContext context = new EvaluatorContext(contextID, this.evaluatorId, this.evaluatorDescriptor, parentID, configurationSerializer, contextControlHandler, this, this.messageDispatcher, this.exceptionCodec);
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
          addEvaluatorContext(new EvaluatorContext(contextID, this.evaluatorId, this.evaluatorDescriptor, parentID, configurationSerializer, contextControlHandler, this, this.messageDispatcher, this.exceptionCodec));
        } else {
          throw new RuntimeException("unknown context signaling state " + contextStatusProto.getContextState());
        }
      }

      final EvaluatorContext context = getEvaluatorContext(contextID);
      final EvaluatorContext parentContext = context.getParentId().isPresent() ?
          getEvaluatorContext(context.getParentId().get()) : null;
      removeEvaluatorContext(context);

      if (ReefServiceProtos.ContextStatusProto.State.FAIL == contextStatusProto.getContextState()) {
        context.onContextFailure(contextStatusProto);
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
      LOG.log(Level.FINEST, "Processing task failure message for Task `{0}`", taskId);
      this.onTaskFailure(taskStatusProto);
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
   * Helper method called in case of Task failure message.
   */
  private void onTaskFailure(final ReefServiceProtos.TaskStatusProto taskStatusProto) {
    assert (ReefServiceProtos.State.FAILED == taskStatusProto.getState());

    this.runningTask = null;

    final String taskId = taskStatusProto.getTaskId();
    final String contextId = taskStatusProto.getContextId();

      /* Assuming nothing went wrong with the context:
       * The failed task could have corrupted it, but I can't make this call.  */
    final Optional<ActiveContext> evaluatorContext = Optional.<ActiveContext>of(getEvaluatorContext(contextId));
    final Optional<byte[]> bytes = taskStatusProto.hasResult() ? Optional.of(taskStatusProto.getResult().toByteArray()) : Optional.<byte[]>empty();
    final Optional<Throwable> exception = this.exceptionCodec.fromBytes(bytes);
    final String message = exception.isPresent() ? exception.get().getMessage() : "No message given";
    final Optional<String> description = Optional.empty();
    final FailedTask failedTask = new FailedTask(taskId, message, description, exception, bytes, evaluatorContext);
    LOG.log(Level.FINEST, "Dispatching FailedTask `{0}`", failedTask);
    this.messageDispatcher.onTaskFailed(failedTask);
  }

  /**
   * Resource status information from the (actual) resource manager.
   */
  public void onResourceStatusMessage(final DriverRuntimeProtocol.ResourceStatusProto resourceStatusProto) {
    synchronized (this.evaluatorDescriptor) {
      LOG.log(Level.FINEST, "Resource manager state update: {0}", resourceStatusProto.getState());

      if ((resourceStatusProto.getState() == ReefServiceProtos.State.DONE ||
          resourceStatusProto.getState() == ReefServiceProtos.State.FAILED) &&
          this.stateManager.isAllocatedOrSubmittedOrRunning()) {
        // something is wrong. The resource manager reports that the Evaluator is done or failed, but the Driver assumes
        // it to be alive.
        final StringBuilder messageBuilder = new StringBuilder();
        if (this.stateManager.isSubmitted()) {
          messageBuilder.append("Evaluator [")
              .append(this.evaluatorId)
              .append("] was submitted for execution, but the resource manager informs me that it is ")
              .append(resourceStatusProto.getState())
              .append(". This most likely means that the Evaluator suffered a failure before establishing a communications link to the driver. ");
        } else if (this.stateManager.isAllocated()) {
          messageBuilder.append("Evaluator [")
              .append(this.evaluatorId)
              .append("] was submitted for execution, but the resource manager informs me that it is ")
              .append(resourceStatusProto.getState())
              .append(". This most likely means that the Evaluator suffered a failure before being used. ");
        } else if (this.stateManager.isRunning()) {
          messageBuilder.append("Evaluator [")
              .append(this.evaluatorId)
              .append("] was running, but the resource manager informs me that it is ")
              .append(resourceStatusProto.getState())
              .append(". This means that the Evaluator failed but wasn't able to send error message back to the driver. ");
        }
        if (null != this.runningTask) {
          messageBuilder.append("Task [")
              .append(this.runningTask.getId())
              .append("] was running when the Evaluator crashed.");
        }
        this.isResourceReleased = true;
        onEvaluatorException(new EvaluatorException(this.evaluatorId, messageBuilder.toString(), this.runningTask));
      }
    }
  }

  @Override
  public String toString() {
    return "EvaluatorManager:"
        + " id=" + this.evaluatorId
        + " state=" + this.stateManager
        + " contexts=" + this.activeContextIds
        + " task=" + this.runningTask;
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
