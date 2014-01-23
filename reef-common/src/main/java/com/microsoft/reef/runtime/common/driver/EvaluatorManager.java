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
import com.microsoft.reef.driver.activity.*;
import com.microsoft.reef.driver.catalog.NodeDescriptor;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.ClosedContext;
import com.microsoft.reef.driver.context.ContextMessage;
import com.microsoft.reef.driver.context.FailedContext;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.CompletedEvaluator;
import com.microsoft.reef.driver.evaluator.EvaluatorType;
import com.microsoft.reef.driver.evaluator.FailedEvaluator;
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
 * The EvaluatorManager uses this to forward Driver messages, launch Activities, and initiate
 * control information (e.g., shutdown, suspend).
 */
@Private
public class EvaluatorManager implements Identifiable, AutoCloseable {

  // Dynamic Parameters
  @NamedParameter(doc = "The Evaluator Identifier.")
  final static class EvaluatorIdentifier implements Name<String> {
  }

  @NamedParameter(doc = "The Evaluator Host.")
  final static class EvaluatorDescriptor implements Name<NodeDescriptor> {
  }

  private final static Logger LOG = Logger.getLogger(EvaluatorManager.class.getName());

  /**
   * Various states that the EvaluatorManager could be in. The EvaluatorManager is
   * created when a resource has been allocated by the ResourceManager.
   */
  static enum STATE {
    ALLOCATED,  // initial state
    SUBMITTED,  // client called AllocatedEvaluator.submitActivity() and we're waiting for first contact
    RUNNING,    // first contact received, all communication channels established, Evaluator sent to client.
    // TODO: Add CLOSING state
    DONE,       // clean shutdown
    FAILED,     // some failure occurred.
    KILLED      // unclean shutdown
  }

  private STATE state = STATE.ALLOCATED;

  private final Clock clock;

  private final RemoteManager remoteManager;

  private final DriverManager driverManager;

  private final ResourceReleaseHandler resourceReleaseHandler;

  private final ResourceLaunchHandler resourceLaunchHandler;

  private final String evaluatorID;

  private final NodeDescriptor nodeDescriptor;

  private final Map<String, EvaluatorContext> activeContextMap = new HashMap<>();

  private final DispatchingEStage dispatcher;

  // TODO: Wrap this in a set-once-with-default class
  private EvaluatorType type = EvaluatorType.JVM;

  // Mutable fields

  private RunningActivity runningActivity = null;

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
      final @Parameter(EvaluatorIdentifier.class) String evaluatorID,
      final @Parameter(EvaluatorDescriptor.class) NodeDescriptor nodeDescriptor,
      final @Parameter(DriverConfigurationOptions.ActiveContextHandlers.class) Set<EventHandler<ActiveContext>> activeContextEventHandlers,
      final @Parameter(DriverConfigurationOptions.ClosedContextHandlers.class) Set<EventHandler<ClosedContext>> closedContextEventHandlers,
      final @Parameter(DriverConfigurationOptions.FailedContextHandlers.class) Set<EventHandler<FailedContext>> failedContextEventHandlers,
      final @Parameter(DriverConfigurationOptions.ContextMessageHandlers.class) Set<EventHandler<ContextMessage>> contextMessageHandlers,
      final @Parameter(DriverConfigurationOptions.RunningActivityHandlers.class) Set<EventHandler<RunningActivity>> runningActivityEventHandlers,
      final @Parameter(DriverConfigurationOptions.CompletedActivityHandlers.class) Set<EventHandler<CompletedActivity>> completedActivityEventHandlers,
      final @Parameter(DriverConfigurationOptions.SuspendedActivityHandlers.class) Set<EventHandler<SuspendedActivity>> suspendedActivityEventHandlers,
      final @Parameter(DriverConfigurationOptions.ActivityMessageHandlers.class) Set<EventHandler<ActivityMessage>> activityMessageEventHandlers,
      final @Parameter(DriverConfigurationOptions.FailedActivityHandlers.class) Set<EventHandler<FailedActivity>> activityExceptionEventHandlers,
      final @Parameter(DriverConfigurationOptions.AllocatedEvaluatorHandlers.class) Set<EventHandler<AllocatedEvaluator>> allocatedEvaluatorEventHandlers,
      final @Parameter(DriverConfigurationOptions.FailedEvaluatorHandlers.class) Set<EventHandler<FailedEvaluator>> failedEvaluatorHandlers,
      final @Parameter(DriverConfigurationOptions.CompletedEvaluatorHandlers.class) Set<EventHandler<CompletedEvaluator>> completedEvaluatorHandlers) {

    this.clock = clock;
    this.remoteManager = remoteManager;
    this.driverManager = driverManager;
    this.resourceReleaseHandler = resourceReleaseHandler;
    this.resourceLaunchHandler = resourceLaunchHandler;
    this.evaluatorID = evaluatorID;
    this.nodeDescriptor = nodeDescriptor;

    this.dispatcher = new DispatchingEStage(errorHandler, 16); // 16 threads

    this.dispatcher.register(ActiveContext.class, activeContextEventHandlers);
    this.dispatcher.register(ClosedContext.class, closedContextEventHandlers);
    this.dispatcher.register(FailedContext.class, failedContextEventHandlers);
    this.dispatcher.register(ContextMessage.class, contextMessageHandlers);

    this.dispatcher.register(RunningActivity.class, runningActivityEventHandlers);
    this.dispatcher.register(CompletedActivity.class, completedActivityEventHandlers);
    this.dispatcher.register(SuspendedActivity.class, suspendedActivityEventHandlers);
    this.dispatcher.register(ActivityMessage.class, activityMessageEventHandlers);
    this.dispatcher.register(FailedActivity.class, activityExceptionEventHandlers);

    this.dispatcher.register(FailedEvaluator.class, failedEvaluatorHandlers);
    this.dispatcher.register(CompletedEvaluator.class, completedEvaluatorHandlers);
    this.dispatcher.register(AllocatedEvaluator.class, allocatedEvaluatorEventHandlers);

    this.dispatcher.onNext(AllocatedEvaluator.class,
        new AllocatedEvaluatorImpl(this, remoteManager.getMyIdentifier()));
  }

  /**
   * @return NodeDescriptor for the node executing this evaluator
   */
  final NodeDescriptor getNodeDescriptor() {
    return this.nodeDescriptor;
  }

  /**
   * @return current running activity, or null if there is not one.
   */
  final RunningActivity getRunningActivity() {
    return this.runningActivity;
  }

  @Override
  public final String getId() {
    return this.evaluatorID;
  }

  public EvaluatorType getType() {
    return type;
  }

  public void setType(final EvaluatorType type) {
    this.type = type;
  }

  public final com.microsoft.reef.driver.evaluator.EvaluatorDescriptor getEvaluatorDescriptor() {
    return new EvaluatorDescriptorImpl(this.nodeDescriptor, this.getType());
  }

  @Override
  public final void close() {
    if (STATE.RUNNING == this.state) {
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
        this.state = STATE.KILLED;
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
                  .setIdentifier(EvaluatorManager.this.evaluatorID).build());
          }
        });
      } catch (final IllegalStateException e) {
        LOG.log(Level.WARNING, "Force resource release because the client closed the clock.", e);
        EvaluatorManager.this.resourceReleaseHandler.onNext(
            DriverRuntimeProtocol.ResourceReleaseProto.newBuilder()
              .setIdentifier(EvaluatorManager.this.evaluatorID).build());
      } finally {
        EvaluatorManager.this.driverManager.release(EvaluatorManager.this);
      }
    }
  }

  /**
   * EvaluatorException will trigger is FailedEvaluator and state transition to FAILED
   *
   * @param exception on the EvaluatorRuntime
   */
  final void handle(final EvaluatorException exception) {
    if (this.state.ordinal() >= STATE.DONE.ordinal()) return;

    LOG.log(Level.FINEST, "EvaluatorManager failing id[{0}]: {1}",
            new Object[] { this.evaluatorID, exception.getMessage() });

    try {
      // TODO: Replace the nulls below.
      this.dispatcher.onNext(FailedEvaluator.class, new FailedEvaluatorImpl(
          exception, null, Optional.ofNullable((FailedActivity) null), this.evaluatorID));
    } catch (final Exception e) {
      LOG.log(Level.SEVERE, "Exception while handling FailedEvaluator", e);
    } finally {
      this.state = STATE.FAILED;
      close();
    }
  }

  final void handle(final RemoteMessage<EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto> evaluatorHeartbeatProtoRemoteMessage) {
    final EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto evaluatorHeartbeatProto = evaluatorHeartbeatProtoRemoteMessage.getMessage();

    if (evaluatorHeartbeatProto.hasEvaluatorStatus()) {
      final ReefServiceProtos.EvaluatorStatusProto status = evaluatorHeartbeatProto.getEvaluatorStatus();
      if (status.hasError()) {
        final ObjectSerializableCodec<Exception> codec = new ObjectSerializableCodec<>();
        handle(new EvaluatorException(getId(), codec.decode(status.getError().toByteArray())));
        return;
      } else if (STATE.SUBMITTED == this.state) {
        final String evaluatorRID = evaluatorHeartbeatProtoRemoteMessage.getIdentifier().toString();
        this.evaluatorControlHandler = remoteManager.getHandler(evaluatorRID, EvaluatorRuntimeProtocol.EvaluatorControlProto.class);
        this.state = STATE.RUNNING;
        LOG.log(Level.FINEST, "Evaluator {0} is running", this.evaluatorID);
      }
    }

    LOG.log(Level.FINEST, "Evaluator heartbeat: {0}", evaluatorHeartbeatProto);

    final ReefServiceProtos.EvaluatorStatusProto evaluatorStatusProto = evaluatorHeartbeatProto.getEvaluatorStatus();

    for (final ReefServiceProtos.ContextStatusProto contextStatusProto : evaluatorHeartbeatProto.getContextStatusList()) {
      handle(contextStatusProto, !evaluatorHeartbeatProto.hasActivityStatus());
    }

    if (evaluatorHeartbeatProto.hasActivityStatus()) {
      handle(evaluatorHeartbeatProto.getActivityStatus());
    }

    if (ReefServiceProtos.State.FAILED == evaluatorStatusProto.getState()) {

      this.state = STATE.FAILED;
      final ObjectSerializableCodec<Exception> codec = new ObjectSerializableCodec<>();
      final EvaluatorException evaluatorException = evaluatorStatusProto.hasError() ?
          new EvaluatorException(this.evaluatorID, codec.decode(evaluatorStatusProto.getError().toByteArray()), this.runningActivity) :
          new EvaluatorException(this.evaluatorID, "unknown cause");

      LOG.log(Level.WARNING, "Failed evaluator: " + getId(), evaluatorException);

      final List<FailedContext> failedContextList = new ArrayList<>();
      for (final ReefServiceProtos.ContextStatusProto contextStatusProto : evaluatorHeartbeatProto.getContextStatusList()) {
        final EvaluatorContext evaluatorContext = this.activeContextMap.get(contextStatusProto.getContextId());
        failedContextList.add(evaluatorContext.getFailedContext(Optional.<ActiveContext>empty(), evaluatorException));
      }

      final Optional<FailedActivity> failedActivityOptional = this.runningActivity != null ?
          Optional.of(new FailedActivity(this.runningActivity.getId(), evaluatorException)) :
          Optional.<FailedActivity>empty();

      dispatcher.onNext(FailedEvaluator.class, new FailedEvaluatorImpl(
          evaluatorException, failedContextList, failedActivityOptional, this.evaluatorID));

      close();
    } else if (ReefServiceProtos.State.DONE == evaluatorStatusProto.getState()) {
      LOG.log(Level.FINEST, "Evaluator {0} done.", getId());
      this.state = STATE.DONE;

      dispatcher.onNext(CompletedEvaluator.class, new CompletedEvaluator() {
        @Override
        public String getId() {
          return EvaluatorManager.this.evaluatorID;
        }
      });

      close();
    }

    LOG.info("DONE with evaluator heartbeat");
  }

  final void handle(final DriverRuntimeProtocol.ResourceLaunchProto resourceLaunchProto) {
    if (STATE.ALLOCATED == this.state) {
      this.state = STATE.SUBMITTED;
      this.resourceLaunchHandler.onNext(resourceLaunchProto);
    } else {
      throw new RuntimeException("Evaluator manager expected " + STATE.ALLOCATED +
          " state but instead is in state " + this.state);
    }
  }


  /**
   * Packages the ActivityControlProto in an EvaluatorControlProto and forward it to the EvaluatorRuntime
   *
   * @param activityControlProto message contains activity control info.
   */
  final void handle(EvaluatorRuntimeProtocol.ContextControlProto activityControlProto) {
    LOG.log(Level.FINEST, "Activity control message from {0}", this.evaluatorID);

    final EvaluatorRuntimeProtocol.EvaluatorControlProto evaluatorControlProto =
        EvaluatorRuntimeProtocol.EvaluatorControlProto.newBuilder()
            .setTimestamp(System.currentTimeMillis())
            .setIdentifier(getId())
            .setContextControl(activityControlProto).build();
    handle(evaluatorControlProto);
  }


  /**
   * Forward the EvaluatorControlProto to the EvaluatorRuntime
   *
   * @param evaluatorControlProto message contains evaluator control information.
   */
  final void handle(final EvaluatorRuntimeProtocol.EvaluatorControlProto evaluatorControlProto) {
    if (STATE.RUNNING == this.state) {
      this.evaluatorControlHandler.onNext(evaluatorControlProto);
    } else {
      throw new RuntimeException("Evaluator manager expects to be in " +
          STATE.RUNNING + " state, but instead is in state " + this.state);
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
      if (!this.activeContextMap.containsKey(contextID)) {
        final EvaluatorContext context = new EvaluatorContext(this, contextID, parentID);
        this.activeContextMap.put(contextID, context);
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
      if (!this.activeContextMap.containsKey(contextID)) {
        if (ReefServiceProtos.ContextStatusProto.State.FAIL == contextStatusProto.getContextState()) {
          // It failed right away
          this.activeContextMap.put(contextID, new EvaluatorContext(this, contextID, parentID));
        } else {
          throw new RuntimeException("unknown context signaling state " + contextStatusProto.getContextState());
        }
      }

      final EvaluatorContext context = this.activeContextMap.remove(contextID);
      final EvaluatorContext parentContext = context.getParentId().isPresent() ?
          this.activeContextMap.get(context.getParentId().get()) : null;

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
   * Handle activity status messages.
   *
   * @param activityStatusProto message contains the current activity status.
   */
  private void handle(final ReefServiceProtos.ActivityStatusProto activityStatusProto) {

    LOG.log(Level.FINEST, "Received activity {0} status {1}",
        new Object[] { activityStatusProto.getActivityId(), activityStatusProto.getState() });

    final String activityId = activityStatusProto.getActivityId();
    final String contextId = activityStatusProto.getContextId();
    final ReefServiceProtos.State activityState = activityStatusProto.getState();

    if (ReefServiceProtos.State.INIT == activityState) {
      final EvaluatorContext evaluatorContext = this.activeContextMap.get(contextId);
      this.runningActivity = new RunningActivityImpl(this, activityId, evaluatorContext);
      this.dispatcher.onNext(RunningActivity.class, this.runningActivity);
    } else if (ReefServiceProtos.State.SUSPEND == activityState) {
      final EvaluatorContext evaluatorContext = this.activeContextMap.get(contextId);
      this.runningActivity = null;
      final byte[] message = activityStatusProto.hasResult() ? activityStatusProto.getResult().toByteArray() : null;
      this.dispatcher.onNext(SuspendedActivity.class,
          new SuspendedActivityImpl(evaluatorContext, message, activityId));
    } else if (ReefServiceProtos.State.DONE == activityState) {
      final EvaluatorContext evaluatorContext = this.activeContextMap.get(contextId);
      this.runningActivity = null;
      final byte[] message = activityStatusProto.hasResult() ? activityStatusProto.getResult().toByteArray() : null;
      this.dispatcher.onNext(CompletedActivity.class,
          new CompletedActivityImpl(evaluatorContext, message, activityId));
    } else if (ReefServiceProtos.State.FAILED == activityState) {
      this.runningActivity = null;
      final ObjectSerializableCodec<Exception> codec = new ObjectSerializableCodec<>();
      /* Assuming nothing went wrong with the context:
       * The failed activity could have corrupted it, but I can't make this call.  */
      final EvaluatorContext evaluatorContext = this.activeContextMap.get(contextId);
      final FailedActivity activityException = activityStatusProto.hasResult() ?
          new FailedActivity(activityId, codec.decode(activityStatusProto.getResult().toByteArray()), Optional.<ActiveContext>of(evaluatorContext)) :
          new FailedActivity(activityId, "Failed Activity: " + activityState, Optional.<ActiveContext>of(evaluatorContext));

      this.dispatcher.onNext(FailedActivity.class, activityException);
    } else if (activityStatusProto.getActivityMessageCount() > 0) {
      assert (this.runningActivity != null);
      for (final ReefServiceProtos.ActivityStatusProto.ActivityMessageProto activityMessageProto : activityStatusProto.getActivityMessageList()) {
        this.dispatcher.onNext(ActivityMessage.class,
            new ActivityMessageImpl(activityMessageProto.getMessage().toByteArray(),
                activityId, contextId, activityMessageProto.getSourceId()));
      }
    }
  }

  /**
   * Resource status information from the (actual) resource manager.
   */
  final void handle(final DriverRuntimeProtocol.ResourceStatusProto resourceStatusProto) {
    LOG.log(Level.FINEST, "Resource manager state update: {0}", resourceStatusProto.getState());

    if (resourceStatusProto.getState() == ReefServiceProtos.State.DONE ||
        resourceStatusProto.getState() == ReefServiceProtos.State.FAILED) {
      if (this.state.ordinal() < STATE.DONE.ordinal()) {
        // something is wrong, I think I'm alive but the resource manager runtime says I'm dead
        final StringBuilder sb = new StringBuilder();
        sb.append("The resource manager informed me that Evaluator " + this.evaluatorID +
            " is in state " + resourceStatusProto.getState() + " but I think I'm in state " + this.state);
        if (resourceStatusProto.getDiagnostics() != null && "".equals(resourceStatusProto.getDiagnostics())) {
          sb.append("Cause: " + resourceStatusProto.getDiagnostics());
        }

        if (runningActivity != null) {
          sb.append("ActivityRuntime " + runningActivity.getId() + " did not complete before this evaluator died. ");
        }
        handle(new EvaluatorException(this.evaluatorID, sb.toString(), runningActivity));
        this.state = STATE.KILLED;
      }
    }
  }
}
