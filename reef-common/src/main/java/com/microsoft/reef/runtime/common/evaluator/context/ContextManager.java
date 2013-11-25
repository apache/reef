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
package com.microsoft.reef.runtime.common.evaluator.context;

import com.google.protobuf.ByteString;
import com.microsoft.reef.annotations.Provided;
import com.microsoft.reef.annotations.audience.EvaluatorSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.proto.EvaluatorRuntimeProtocol;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.runtime.common.evaluator.HeartBeatManager;
import com.microsoft.reef.runtime.common.evaluator.activity.ActivityClientCodeException;
import com.microsoft.reef.util.Optional;
import com.microsoft.reef.util.TANGUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.InjectionFuture;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Stack;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages the stack of context in the Evaluator.
 */
@Private
@EvaluatorSide
@Provided
public final class ContextManager implements AutoCloseable {
  private static final Logger LOG = Logger.getLogger(ContextManager.class.getName());
  /**
   * The stack of context.
   */
  private final Stack<ContextRuntime> contextStack = new Stack<>();
  /**
   * Used to instantiate the root context.
   */
  private final InjectionFuture<RootContextLauncher> launchContext;
  /**
   * Used for status reporting to the Driver.
   */
  private final HeartBeatManager heartBeatManager;

  /**
   * @param launchContext    to instantiate the root context.
   * @param heartBeatManager for status reporting to the Driver.
   */
  @Inject
  ContextManager(final InjectionFuture<RootContextLauncher> launchContext, final HeartBeatManager heartBeatManager) {
    this.launchContext = launchContext;
    this.heartBeatManager = heartBeatManager;
  }

  /**
   * Start the context manager. This initiates the root context.
   *
   * @throws ContextClientCodeException if the root context can't be instantiated.
   */
  public void start() throws ContextClientCodeException {
    synchronized (this.contextStack) {
      LOG.log(Level.FINEST, "Instantiating root context.");
      this.contextStack.push(this.launchContext.get().getRootContext());

      if (this.launchContext.get().getInitialActivityConfiguration().isPresent()) {
        LOG.log(Level.FINEST, "Launching the initial Activity");
        try {
          this.contextStack.peek().startActivity(this.launchContext.get().getInitialActivityConfiguration().get());
        } catch (final ActivityClientCodeException e) {
          this.handleActivityException(e);
        }
      }
    }
  }

  /**
   * Shuts down. This forecefully kills the Activity if there is one and then shuts down all Contexts on the stack,
   * starting at the top.
   */
  @Override
  public void close() {
    synchronized (this.contextStack) {
      this.contextStack.lastElement().close();
    }
  }

  /**
   * @return true if there is no context.
   */
  // TODO: Do we really need this?
  public boolean contextStackIsEmpty() {
    synchronized (this.contextStack) {
      return this.contextStack.isEmpty();
    }
  }

  /**
   * Processes the given ActivityControlProto to launch / close / suspend Activities and Contexts.
   * <p/>
   * This also triggers the HeartBeatManager to send a heartbeat with the result of this operation.
   *
   * @param controlMessage the message to process
   */
  public void handleActivityControl(final EvaluatorRuntimeProtocol.ContextControlProto controlMessage) {

    synchronized (this.heartBeatManager) {
      try {
        final byte[] message = controlMessage.hasActivityMessage() ? controlMessage.getActivityMessage().toByteArray() : null;
        if (controlMessage.hasAddContext() && controlMessage.hasRemoveContext()) {
          throw new IllegalArgumentException("Received a message with both add and remove context. This is unsupported.");
        }

        if (controlMessage.hasAddContext()) {
          this.addContext(controlMessage.getAddContext());
          if (controlMessage.hasStartActivity()) {
            // We support submitContextAndActivity()
            this.startActivity(controlMessage.getStartActivity());
          } else {
            // We need to trigger a heartbeat here. In other cases, the heartbeat will be triggered by the ActivityRuntime
            // Therefore this call can not go into addContext
            this.heartBeatManager.onNext();
          }
        } else if (controlMessage.hasRemoveContext()) {
          this.removeContext(controlMessage.getRemoveContext().getContextId());
        } else if (controlMessage.hasStartActivity()) {
          this.startActivity(controlMessage.getStartActivity());
        } else if (controlMessage.hasStopActivity()) {
          this.contextStack.peek().closeActivity(message);
        } else if (controlMessage.hasSuspendActivity()) {
          this.contextStack.peek().suspendActivity(message);
        } else if (controlMessage.hasActivityMessage()) {
          this.contextStack.peek().deliverActivityMessage(message);
        } else if (controlMessage.hasContextMessage()) {
          final EvaluatorRuntimeProtocol.ContextMessageProto contextMessageProto = controlMessage.getContextMessage();
          boolean deliveredMessage = false;
          for (final ContextRuntime context : this.contextStack) {
            if (context.getIdentifier().equals(contextMessageProto.getContextId())) {
              context.handleContextMessage(contextMessageProto.getMessage().toByteArray());
              deliveredMessage = true;
              break;
            }
          }
          if (!deliveredMessage) {
            throw new IllegalStateException("Sent message to unknown context " + contextMessageProto.getContextId());
          }
        } else {
          throw new RuntimeException("Unknown activity control message: " + controlMessage.toString());
        }
      } catch (final ActivityClientCodeException e) {
        this.handleActivityException(e);
      } catch (final ContextClientCodeException e) {
        this.handleContextException(e);
      }
    }

  }

  /**
   * @return the ActivityStatusProto of the currently running activity, if there is any
   */
  public Optional<ReefServiceProtos.ActivityStatusProto> getActivityStatus() {
    synchronized (this.contextStack) {
      if (this.contextStack.isEmpty()) {
        throw new RuntimeException("Asked for an Activity status while there isn't even a context running.");
      }
      return this.contextStack.peek().getActivityStatus();
    }
  }

  /**
   * @return the status of all context in the stack.
   */
  public Collection<ReefServiceProtos.ContextStatusProto> getContextStatusCollection() {
    synchronized (this.contextStack) {
      final ArrayList<ReefServiceProtos.ContextStatusProto> result = new ArrayList<>(this.contextStack.size());
      for (final ContextRuntime contextRuntime : this.contextStack) {
        final ReefServiceProtos.ContextStatusProto contextStatusProto = contextRuntime.getContextStatus();
        LOG.log(Level.FINEST, "Add context status: " + contextStatusProto);
        result.add(contextStatusProto);
      }
      return result;
    }
  }

  /**
   * Add a context to the stack.
   *
   * @param addContextProto
   * @throws ContextClientCodeException if there is a client code related issue.
   */
  private void addContext(final EvaluatorRuntimeProtocol.AddContextProto addContextProto) throws ContextClientCodeException {
    synchronized (this.contextStack) {
      final ContextRuntime currentTopContext = this.contextStack.peek();
      if (!currentTopContext.getIdentifier().equals(addContextProto.getParentContextId())) {
        throw new IllegalStateException("Trying to instantiate a child context on context with id `" +
            addContextProto.getParentContextId() + "` while the current top context id is `" +
            currentTopContext.getIdentifier() +
            "`");
      }
      final Configuration contextConfiguration = TANGUtils.fromString(addContextProto.getContextConfiguration());
      final ContextRuntime newTopContext;
      if (addContextProto.hasServiceConfiguration()) {
        newTopContext = currentTopContext.spawnChildContext(contextConfiguration,
            TANGUtils.fromString(addContextProto.getServiceConfiguration()));

      } else {
        newTopContext = currentTopContext.spawnChildContext(contextConfiguration);
      }
      this.contextStack.push(newTopContext);
    }
  }

  /**
   * Remove the context with the given ID from the stack.
   *
   * @param contextID
   * @throws IllegalStateException if the given ID does not refer to the top of stack.
   */
  private void removeContext(final String contextID) {
    synchronized (this.contextStack) {
      if (!contextID.equals(this.contextStack.peek().getIdentifier())) {
        throw new IllegalStateException("Trying to close context with id `" + contextID +
            "`. But the top context has id `" +
            this.contextStack.peek().getIdentifier() + "`");
      }

      this.contextStack.peek().close();
      if (this.contextStack.size() > 1) {
        /* We did not close the root context. Therefore, we need to inform the
         * driver explicitly that this context is closed. The root context notification
         * is implicit in the Evaluator close/done notification.
         */
        this.heartBeatManager.onNext(); // Ensure Driver gets notified of context DONE state
      }
      this.contextStack.pop();
      System.gc(); // TODO sure??
    }
  }

  /**
   * Launch an Activity.
   *
   * @param startActivityProto
   * @throws ActivityClientCodeException
   */
  private void startActivity(final EvaluatorRuntimeProtocol.StartActivityProto startActivityProto) throws ActivityClientCodeException {
    synchronized (this.contextStack) {
      final ContextRuntime currentActiveContext = this.contextStack.peek();

      final String expectedContextId = startActivityProto.getContextId();
      if (!expectedContextId.equals(currentActiveContext.getIdentifier())) {
        throw new IllegalStateException("Activity expected context `" + expectedContextId +
            "` but the active context has ID `" + currentActiveContext.getIdentifier() + "`");
      }

      final Configuration activityConfiguration = TANGUtils.fromString(startActivityProto.getConfiguration());
      currentActiveContext.startActivity(activityConfiguration);
    }
  }

  /**
   * THIS ASSUMES THAT IT IS CALLED ON A THREAD HOLDING THE LOCK ON THE HeartBeatManager
   *
   * @param e
   */
  private void handleActivityException(final ActivityClientCodeException e) {
    LOG.log(Level.SEVERE, "ActivityClientCodeException", e);
    final ByteString exception = ByteString.copyFrom(new ObjectSerializableCodec<Throwable>().encode(e.getCause()));
    final ReefServiceProtos.ActivityStatusProto activityStatus = ReefServiceProtos.ActivityStatusProto.newBuilder()
        .setContextId(e.getContextID())
        .setActivityId(e.getActivityID())
        .setResult(exception)
        .setState(ReefServiceProtos.State.FAILED)
        .build();
    LOG.log(Level.SEVERE, "Sending heartbeat: " + activityStatus.toString());
    this.heartBeatManager.onNext(activityStatus);
  }

  /**
   * THIS ASSUMES THAT IT IS CALLED ON A THREAD HOLDING THE LOCK ON THE HeartBeatManager
   *
   * @param e
   */
  private void handleContextException(final ContextClientCodeException e) {
    LOG.log(Level.SEVERE, "ContextClientCodeException", e);

    final ByteString exception = ByteString.copyFrom(new ObjectSerializableCodec<Throwable>().encode(e.getCause()));
    final ReefServiceProtos.ContextStatusProto.Builder contextStatusBuilder = ReefServiceProtos.ContextStatusProto.newBuilder()
        .setContextId(e.getContextID())
        .setContextState(ReefServiceProtos.ContextStatusProto.State.FAIL)
        .setError(exception);

    if (e.getParentID().isPresent()) {
      contextStatusBuilder.setParentId(e.getParentID().get());
    }

    final ReefServiceProtos.ContextStatusProto contextStatus = contextStatusBuilder.build();

    LOG.log(Level.SEVERE, "Sending heartbeat: " + contextStatus.toString());

    this.heartBeatManager.onNext(contextStatus);
  }

}
