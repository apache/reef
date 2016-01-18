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
package org.apache.reef.runtime.common.evaluator.context;

import com.google.protobuf.ByteString;
import org.apache.reef.annotations.Provided;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.evaluator.context.ContextMessage;
import org.apache.reef.evaluator.context.ContextMessageSource;
import org.apache.reef.evaluator.context.parameters.Services;
import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.runtime.common.evaluator.task.TaskClientCodeException;
import org.apache.reef.runtime.common.evaluator.task.TaskRuntime;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.Optional;

import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The evaluator side resourcemanager for Contexts.
 */
@Provided
@Private
@EvaluatorSide
public final class ContextRuntime {

  private static final Logger LOG = Logger.getLogger(ContextRuntime.class.getName());

  /**
   * Context-local injector. This contains information that will not be available in child injectors.
   */
  private final Injector contextInjector;

  /**
   * Service injector. State in this injector moves to child injectors.
   */
  private final Injector serviceInjector;

  /**
   * Convenience class to hold all the event handlers for the context as well as the service instances.
   */
  private final ContextLifeCycle contextLifeCycle;
  /**
   * The parent context, if there is any.
   */
  private final Optional<ContextRuntime> parentContext; // guarded by this
  /**
   * The child context, if there is any.
   */
  private Optional<ContextRuntime> childContext = Optional.empty(); // guarded by this
  /**
   * The currently running task, if there is any.
   */
  private Optional<TaskRuntime> task = Optional.empty(); // guarded by this

  private Thread taskRuntimeThread = null;

  // TODO[JIRA REEF-835]: Which lock guards this?
  private ReefServiceProtos.ContextStatusProto.State contextState =
      ReefServiceProtos.ContextStatusProto.State.READY;

  /**
   * Create a new ContextRuntime.
   *
   * @param serviceInjector      the serviceInjector to be used.
   * @param contextConfiguration the Configuration for this context.
   * @throws ContextClientCodeException if the context cannot be instantiated.
   */
  ContextRuntime(final Injector serviceInjector, final Configuration contextConfiguration,
                 final Optional<ContextRuntime> parentContext) throws ContextClientCodeException {

    this.serviceInjector = serviceInjector;
    this.parentContext = parentContext;

    // Trigger the instantiation of the services
    try {

      final Set<Object> services = serviceInjector.getNamedInstance(Services.class);
      this.contextInjector = serviceInjector.forkInjector(contextConfiguration);

      this.contextLifeCycle = this.contextInjector.getInstance(ContextLifeCycle.class);

    } catch (BindException | InjectionException e) {

      final Optional<String> parentID = this.getParentContext().isPresent() ?
          Optional.of(this.getParentContext().get().getIdentifier()) :
          Optional.<String>empty();

      throw new ContextClientCodeException(
          ContextClientCodeException.getIdentifier(contextConfiguration),
          parentID, "Unable to spawn context", e);
    }

    // Trigger the context start events on contextInjector.
    this.contextLifeCycle.start();
  }

  /**
   * Create a new ContextRuntime for the root context.
   *
   * @param serviceInjector      the serviceInjector to be used.
   * @param contextConfiguration the Configuration for this context.
   * @throws ContextClientCodeException if the context cannot be instantiated.
   */
  ContextRuntime(final Injector serviceInjector,
                 final Configuration contextConfiguration) throws ContextClientCodeException {
    this(serviceInjector, contextConfiguration, Optional.<ContextRuntime>empty());
    LOG.log(Level.FINEST, "Instantiating root context");
  }


  /**
   * Spawns a new context.
   * <p>
   * The new context will have a serviceInjector that is created by forking the one in this object with the given
   * serviceConfiguration. The contextConfiguration is used to fork the contextInjector from that new serviceInjector.
   *
   * @param contextConfiguration the new context's context (local) Configuration.
   * @param serviceConfiguration the new context's service Configuration.
   * @return a child context.
   * @throws ContextClientCodeException If the context can't be instantiate due to user code / configuration issues
   * @throws IllegalStateException      If this method is called when there is either a task or child context already
   *                                    present.
   */
  ContextRuntime spawnChildContext(
      final Configuration contextConfiguration,
      final Configuration serviceConfiguration) throws ContextClientCodeException {

    synchronized (this.contextLifeCycle) {

      if (this.task.isPresent()) {
        throw new IllegalStateException(
            "Attempting to spawn a child context when a Task with id '" +
                this.task.get().getId() + "' is running.");
      }

      if (this.childContext.isPresent()) {
        throw new IllegalStateException(
            "Attempting to instantiate a child context on a context that is not the topmost active context");
      }

      try {

        final Injector childServiceInjector =
            this.serviceInjector.forkInjector(serviceConfiguration);

        final ContextRuntime newChildContext =
            new ContextRuntime(childServiceInjector, contextConfiguration, Optional.of(this));

        this.childContext = Optional.of(newChildContext);
        return newChildContext;

      } catch (final BindException e) {

        final Optional<String> parentID = this.getParentContext().isPresent() ?
            Optional.of(this.getParentContext().get().getIdentifier()) :
            Optional.<String>empty();

        throw new ContextClientCodeException(
            ContextClientCodeException.getIdentifier(contextConfiguration),
            parentID, "Unable to spawn context", e);
      }
    }
  }

  /**
   * Spawns a new context without services of its own.
   * <p>
   * The new context will have a serviceInjector that is created by forking the one in this object. The
   * contextConfiguration is used to fork the contextInjector from that new serviceInjector.
   *
   * @param contextConfiguration the new context's context (local) Configuration.
   * @return a child context.
   * @throws ContextClientCodeException If the context can't be instantiate due to user code / configuration issues.
   * @throws IllegalStateException      If this method is called when there is either a task or child context already
   *                                    present.
   */
  ContextRuntime spawnChildContext(
      final Configuration contextConfiguration) throws ContextClientCodeException {

    synchronized (this.contextLifeCycle) {

      if (this.task.isPresent()) {
        throw new IllegalStateException(
            "Attempting to to spawn a child context while a Task with id '" +
                this.task.get().getId() + "' is running.");
      }

      if (this.childContext.isPresent()) {
        throw new IllegalStateException(
            "Attempting to spawn a child context on a context that is not the topmost active context");
      }

      final Injector childServiceInjector = this.serviceInjector.forkInjector();
      final ContextRuntime newChildContext =
          new ContextRuntime(childServiceInjector, contextConfiguration, Optional.of(this));

      this.childContext = Optional.of(newChildContext);
      return newChildContext;
    }
  }

  /**
   * Launches a Task on this context.
   *
   * @param taskConfig the configuration to be used for the task.
   * @throws org.apache.reef.runtime.common.evaluator.task.TaskClientCodeException If the Task cannot be instantiated
   * due to user code / configuration issues.
   * @throws IllegalStateException                                                 If this method is called when
   * there is either a task or child context already present.
   */
  @SuppressWarnings("checkstyle:illegalcatch")
  void startTask(final Configuration taskConfig) throws TaskClientCodeException {

    synchronized (this.contextLifeCycle) {

      if (this.task.isPresent() && this.task.get().hasEnded()) {
        // clean up state
        this.task = Optional.empty();
      }

      if (this.task.isPresent()) {
        throw new IllegalStateException("Attempting to start a Task when a Task with id '" +
            this.task.get().getId() + "' is running.");
      }

      if (this.childContext.isPresent()) {
        throw new IllegalStateException(
            "Attempting to start a Task on a context that is not the topmost active context");
      }

      try {
        final Injector taskInjector = this.contextInjector.forkInjector(taskConfig);
        final TaskRuntime taskRuntime = taskInjector.getInstance(TaskRuntime.class);
        taskRuntime.initialize();
        this.taskRuntimeThread = new Thread(taskRuntime, taskRuntime.getId());
        this.taskRuntimeThread.start();
        this.task = Optional.of(taskRuntime);
        LOG.log(Level.FINEST, "Started task: {0}", taskRuntime.getTaskId());
      } catch (final BindException | InjectionException e) {
        throw new TaskClientCodeException(TaskClientCodeException.getTaskId(taskConfig),
            this.getIdentifier(),
            "Unable to instantiate the new task", e);
      } catch (final Throwable t) {
        throw new TaskClientCodeException(TaskClientCodeException.getTaskId(taskConfig),
            this.getIdentifier(),
            "Unable to start the new task", t);
      }
    }
  }

  /**
   * Close this context. If there is a child context, this recursively closes it before closing this context. If
   * there is a Task currently running, that will be closed.
   */
  void close() {

    synchronized (this.contextLifeCycle) {

      this.contextState = ReefServiceProtos.ContextStatusProto.State.DONE;

      if (this.task.isPresent()) {
        LOG.log(Level.WARNING, "Shutting down a task because the underlying context is being closed.");
        this.task.get().close(null);
      }

      if (this.childContext.isPresent()) {
        LOG.log(Level.WARNING, "Closing a context because its parent context is being closed.");
        this.childContext.get().close();
      }

      this.contextLifeCycle.close();

      if (this.parentContext.isPresent()) {
        this.parentContext.get().resetChildContext();
      }
    }
  }

  /**
   * @return the parent context, if there is one.
   */
  Optional<ContextRuntime> getParentContext() {
    return this.parentContext;
  }

  /**
   * Deliver the given message to the Task.
   * <p>
   * Note that due to races, the task might have already ended. In that case, we drop this call and leave a WARNING
   * in the log.
   *
   * @param message the suspend message to deliver or null if there is none.
   */
  void suspendTask(final byte[] message) {
    synchronized (this.contextLifeCycle) {
      if (!this.task.isPresent()) {
        LOG.log(Level.WARNING, "Received a suspend task while there was no task running. Ignoring.");
      } else {
        this.task.get().suspend(message);
      }
    }
  }

  /**
   * Issue a close call to the Task
   * <p>
   * Note that due to races, the task might have already ended. In that case, we drop this call and leave a WARNING
   * in the log.
   *
   * @param message the close  message to deliver or null if there is none.
   */
  void closeTask(final byte[] message) {
    synchronized (this.contextLifeCycle) {
      if (!this.task.isPresent()) {
        LOG.log(Level.WARNING, "Received a close task while there was no task running. Ignoring.");
      } else {
        this.task.get().close(message);
      }
    }
  }

  /**
   * Deliver a message to the Task
   * <p>
   * Note that due to races, the task might have already ended. In that case, we drop this call and leave a WARNING
   * in the log.
   *
   * @param message the close  message to deliver or null if there is none.
   */
  void deliverTaskMessage(final byte[] message) {
    synchronized (this.contextLifeCycle) {
      if (!this.task.isPresent()) {
        LOG.log(Level.WARNING, "Received a task message while there was no task running. Ignoring.");
      } else {
        this.task.get().deliver(message);
      }
    }
  }

  /**
   * @return the identifier of this context.
   */
  String getIdentifier() {
    return this.contextLifeCycle.getIdentifier();
  }

  /**
   * Handle the context message.
   *
   * @param message sent by the driver
   */
  void handleContextMessage(final byte[] message) {
    this.contextLifeCycle.handleContextMessage(message);
  }

  /**
   * @return the state of the running Task, if one is running.
   */
  Optional<ReefServiceProtos.TaskStatusProto> getTaskStatus() {
    synchronized (this.contextLifeCycle) {
      if (this.task.isPresent()) {
        if (this.task.get().hasEnded()) {
          this.task = Optional.empty();
          return Optional.empty();
        } else {
          return Optional.of(this.task.get().getStatusProto());
        }
      } else {
        return Optional.empty();
      }
    }
  }

  /**
   * Called by the child context when it has been closed.
   */
  private void resetChildContext() {
    synchronized (this.contextLifeCycle) {
      if (this.childContext.isPresent()) {
        this.childContext = Optional.empty();
      } else {
        throw new IllegalStateException("no child context set");
      }
    }
  }

  /**
   * @return this context's status in protocol buffer form.
   */
  ReefServiceProtos.ContextStatusProto getContextStatus() {

    synchronized (this.contextLifeCycle) {

      final ReefServiceProtos.ContextStatusProto.Builder builder =
          ReefServiceProtos.ContextStatusProto.newBuilder()
              .setContextId(this.getIdentifier())
              .setContextState(this.contextState);

      if (this.parentContext.isPresent()) {
        builder.setParentId(this.parentContext.get().getIdentifier());
      }

      for (final ContextMessageSource contextMessageSource : this.contextLifeCycle.getContextMessageSources()) {
        final Optional<ContextMessage> contextMessageOptional = contextMessageSource.getMessage();
        if (contextMessageOptional.isPresent()) {
          builder.addContextMessage(ReefServiceProtos.ContextStatusProto.ContextMessageProto.newBuilder()
              .setSourceId(contextMessageOptional.get().getMessageSourceID())
              .setMessage(ByteString.copyFrom(contextMessageOptional.get().get()))
              .build());
        }
      }

      return builder.build();
    }
  }
}
