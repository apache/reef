/**
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
package org.apache.reef.runtime.common.evaluator.context;

import com.google.protobuf.ByteString;
import org.apache.reef.annotations.Provided;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.proto.EvaluatorRuntimeProtocol;
import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.runtime.common.evaluator.HeartBeatManager;
import org.apache.reef.runtime.common.evaluator.task.TaskClientCodeException;
import org.apache.reef.runtime.common.utils.ExceptionCodec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.util.Optional;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
   * To serialize Configurations.
   */
  private final ConfigurationSerializer configurationSerializer;

  private final ExceptionCodec exceptionCodec;

  /**
   * @param launchContext           to instantiate the root context.
   * @param heartBeatManager        for status reporting to the Driver.
   * @param configurationSerializer
   * @param exceptionCodec
   */
  @Inject
  ContextManager(final InjectionFuture<RootContextLauncher> launchContext,
                 final HeartBeatManager heartBeatManager,
                 final ConfigurationSerializer configurationSerializer,
                 final ExceptionCodec exceptionCodec) {
    this.launchContext = launchContext;
    this.heartBeatManager = heartBeatManager;
    this.configurationSerializer = configurationSerializer;
    this.exceptionCodec = exceptionCodec;
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

      if (this.launchContext.get().getInitialTaskConfiguration().isPresent()) {
        LOG.log(Level.FINEST, "Launching the initial Task");
        try {
          this.contextStack.peek().startTask(this.launchContext.get().getInitialTaskConfiguration().get());
        } catch (final TaskClientCodeException e) {
          this.handleTaskException(e);
        }
      }
    }
  }

  /**
   * Shuts down. This forecefully kills the Task if there is one and then shuts down all Contexts on the stack,
   * starting at the top.
   */
  @Override
  public void close() {
    synchronized (this.contextStack) {
      if (!this.contextStackIsEmpty()) {
        this.contextStack.lastElement().close();
      }
    }
  }

  /**
   * @return true if there is no context.
   */
  public boolean contextStackIsEmpty() {
    synchronized (this.contextStack) {
      return this.contextStack.isEmpty();
    }
  }

  /**
   * Processes the given ContextControlProto to launch / close / suspend Tasks and Contexts.
   * <p/>
   * This also triggers the HeartBeatManager to send a heartbeat with the result of this operation.
   *
   * @param controlMessage the message to process
   */
  public void handleContextControlProtocol(
      final EvaluatorRuntimeProtocol.ContextControlProto controlMessage) {

    synchronized (this.heartBeatManager) {
      try {
        if (controlMessage.hasAddContext() && controlMessage.hasRemoveContext()) {
          throw new IllegalArgumentException(
              "Received a message with both add and remove context. This is unsupported.");
        }

        final byte[] message = controlMessage.hasTaskMessage() ?
            controlMessage.getTaskMessage().toByteArray() : null;

        if (controlMessage.hasAddContext()) {
          this.addContext(controlMessage.getAddContext());
          if (controlMessage.hasStartTask()) {
            // We support submitContextAndTask()
            this.startTask(controlMessage.getStartTask());
          } else {
            // We need to trigger a heartbeat here.
            // In other cases, the heartbeat will be triggered by the TaskRuntime
            // Therefore this call can not go into addContext.
            this.heartBeatManager.sendHeartbeat();
          }
        } else if (controlMessage.hasRemoveContext()) {
          this.removeContext(controlMessage.getRemoveContext().getContextId());
        } else if (controlMessage.hasStartTask()) {
          this.startTask(controlMessage.getStartTask());
        } else if (controlMessage.hasStopTask()) {
          this.contextStack.peek().closeTask(message);
        } else if (controlMessage.hasSuspendTask()) {
          this.contextStack.peek().suspendTask(message);
        } else if (controlMessage.hasTaskMessage()) {
          this.contextStack.peek().deliverTaskMessage(message);
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
            throw new IllegalStateException(
                "Sent message to unknown context " + contextMessageProto.getContextId());
          }
        } else {
          throw new RuntimeException("Unknown task control message: " + controlMessage);
        }
      } catch (final TaskClientCodeException e) {
        this.handleTaskException(e);
      } catch (final ContextClientCodeException e) {
        this.handleContextException(e);
      }
    }

  }

  /**
   * @return the TaskStatusProto of the currently running task, if there is any
   */
  public Optional<ReefServiceProtos.TaskStatusProto> getTaskStatus() {
    synchronized (this.contextStack) {
      if (this.contextStack.isEmpty()) {
        throw new RuntimeException(
            "Asked for a Task status while there isn't even a context running.");
      }
      return this.contextStack.peek().getTaskStatus();
    }
  }

  /**
   * @return the status of all context in the stack.
   */
  public Collection<ReefServiceProtos.ContextStatusProto> getContextStatusCollection() {
    synchronized (this.contextStack) {
      final List<ReefServiceProtos.ContextStatusProto> result = new ArrayList<>(this.contextStack.size());
      for (final ContextRuntime contextRuntime : this.contextStack) {
        final ReefServiceProtos.ContextStatusProto contextStatusProto = contextRuntime.getContextStatus();
        LOG.log(Level.FINEST, "Add context status: {0}", contextStatusProto);
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
  private void addContext(
      final EvaluatorRuntimeProtocol.AddContextProto addContextProto)
      throws ContextClientCodeException {

    synchronized (this.contextStack) {
      try {

        final ContextRuntime currentTopContext = this.contextStack.peek();

        if (!currentTopContext.getIdentifier().equals(addContextProto.getParentContextId())) {
          throw new IllegalStateException("Trying to instantiate a child context on context with id `" +
              addContextProto.getParentContextId() + "` while the current top context id is `" +
              currentTopContext.getIdentifier() + "`");
        }

        final Configuration contextConfiguration =
            this.configurationSerializer.fromString(addContextProto.getContextConfiguration());

        final ContextRuntime newTopContext;
        if (addContextProto.hasServiceConfiguration()) {
          newTopContext = currentTopContext.spawnChildContext(contextConfiguration,
              this.configurationSerializer.fromString(addContextProto.getServiceConfiguration()));
        } else {
          newTopContext = currentTopContext.spawnChildContext(contextConfiguration);
        }

        this.contextStack.push(newTopContext);

      } catch (final IOException | BindException e) {
        throw new RuntimeException("Unable to read configuration.", e);
      }

    }
  }

  /**
   * Remove the context with the given ID from the stack.
   *
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
        this.heartBeatManager.sendHeartbeat(); // Ensure Driver gets notified of context DONE state
      }
      this.contextStack.pop();
      System.gc(); // TODO sure??
    }
  }

  /**
   * Launch a Task.
   */
  private void startTask(
      final EvaluatorRuntimeProtocol.StartTaskProto startTaskProto) throws TaskClientCodeException {

    synchronized (this.contextStack) {

      final ContextRuntime currentActiveContext = this.contextStack.peek();

      final String expectedContextId = startTaskProto.getContextId();
      if (!expectedContextId.equals(currentActiveContext.getIdentifier())) {
        throw new IllegalStateException("Task expected context `" + expectedContextId +
            "` but the active context has ID `" + currentActiveContext.getIdentifier() + "`");
      }

      try {
        final Configuration taskConfig =
            this.configurationSerializer.fromString(startTaskProto.getConfiguration());
        currentActiveContext.startTask(taskConfig);
      } catch (IOException | BindException e) {
        throw new RuntimeException("Unable to read configuration.", e);
      }
    }
  }

  /**
   * THIS ASSUMES THAT IT IS CALLED ON A THREAD HOLDING THE LOCK ON THE HeartBeatManager
   */
  private void handleTaskException(final TaskClientCodeException e) {

    LOG.log(Level.SEVERE, "TaskClientCodeException", e);

    final ByteString exception = ByteString.copyFrom(this.exceptionCodec.toBytes(e.getCause()));

    final ReefServiceProtos.TaskStatusProto taskStatus =
        ReefServiceProtos.TaskStatusProto.newBuilder()
            .setContextId(e.getContextId())
            .setTaskId(e.getTaskId())
            .setResult(exception)
            .setState(ReefServiceProtos.State.FAILED)
            .build();

    LOG.log(Level.SEVERE, "Sending heartbeat: {0}", taskStatus);

    this.heartBeatManager.sendTaskStatus(taskStatus);
  }

  /**
   * THIS ASSUMES THAT IT IS CALLED ON A THREAD HOLDING THE LOCK ON THE HeartBeatManager
   */
  private void handleContextException(final ContextClientCodeException e) {

    LOG.log(Level.SEVERE, "ContextClientCodeException", e);

    final ByteString exception = ByteString.copyFrom(this.exceptionCodec.toBytes(e.getCause()));

    final ReefServiceProtos.ContextStatusProto.Builder contextStatusBuilder =
        ReefServiceProtos.ContextStatusProto.newBuilder()
            .setContextId(e.getContextID())
            .setContextState(ReefServiceProtos.ContextStatusProto.State.FAIL)
            .setError(exception);

    if (e.getParentID().isPresent()) {
      contextStatusBuilder.setParentId(e.getParentID().get());
    }

    final ReefServiceProtos.ContextStatusProto contextStatus = contextStatusBuilder.build();

    LOG.log(Level.SEVERE, "Sending heartbeat: {0}", contextStatus);

    this.heartBeatManager.sendContextStatus(contextStatus);
  }
}
