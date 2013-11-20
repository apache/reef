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
import com.microsoft.reef.driver.contexts.ServiceConfiguration;
import com.microsoft.reef.evaluator.context.ContextMessage;
import com.microsoft.reef.evaluator.context.ContextMessageSource;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.runtime.common.evaluator.activity.ActivityClientCodeException;
import com.microsoft.reef.runtime.common.evaluator.activity.ActivityRuntime;
import com.microsoft.reef.util.Optional;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;

import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The evaluator side runtime for Contexts.
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
   * The child context, if there is any.
   */
  private Optional<ContextRuntime> childContext = Optional.empty(); // guarded by this
  /**
   * The parent context, if there is any.
   */
  private final Optional<ContextRuntime> parentContext; // guarded by this
  /**
   * The currently running activity, if there is any.
   */
  private Optional<ActivityRuntime> activity = Optional.empty(); // guarded by this

  // TODO: Which lock guards this?
  private ReefServiceProtos.ContextStatusProto.State contextState = ReefServiceProtos.ContextStatusProto.State.READY;

  /**
   * Create a new ContextRuntime.
   *
   * @param serviceInjector      the serviceInjector to be used.
   * @param contextConfiguration the Configuration for this context.
   * @throws ContextClientCodeException if the context cannot be instantiated.
   */
  ContextRuntime(final Injector serviceInjector, final Configuration contextConfiguration, final Optional<ContextRuntime> parentContext)
      throws ContextClientCodeException {
    this.serviceInjector = serviceInjector;
    this.parentContext = parentContext;
    // Trigger the instantiation of the services
    try {
      final Set<Object> services = serviceInjector.getNamedInstance(ServiceConfiguration.Services.class);
      this.contextInjector = serviceInjector.forkInjector(contextConfiguration);

      this.contextLifeCycle = this.contextInjector.getInstance(ContextLifeCycle.class);
    } catch (BindException | InjectionException e) {
      final Optional<String> parentID = this.getParentContext().isPresent() ?
          Optional.of(this.getParentContext().get().getIdentifier()) :
          Optional.<String>empty();
      throw new ContextClientCodeException(ContextClientCodeException.getIdentifier(contextConfiguration), parentID, "Unable to spawn context", e);
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
  ContextRuntime(final Injector serviceInjector, final Configuration contextConfiguration) throws ContextClientCodeException {
    this(serviceInjector, contextConfiguration, Optional.<ContextRuntime>empty());
    LOG.log(Level.INFO, "Instantiating root context");
  }


  /**
   * Spawns a new context.
   * <p/>
   * The new context will have a serviceInjector that is created by forking the one in this object with the given
   * serviceConfiguration. The contextConfiguration is used to fork the contextInjector from that new serviceInjector.
   *
   * @param contextConfiguration the new context's context (local) Configuration.
   * @param serviceConfiguration the new context's service Configuration.
   * @return a child context.
   * @throws ContextClientCodeException If the context can't be instantiate due to user code / configuration issues
   * @throws IllegalStateException      If this method is called when there is either an activity or child context already
   *                                    present.
   */
  ContextRuntime spawnChildContext(final Configuration contextConfiguration, final Configuration serviceConfiguration)
      throws ContextClientCodeException {

    synchronized (this.contextLifeCycle) {
      if (this.activity.isPresent()) {
        throw new IllegalStateException("Attempting to spawn a child context when an Activity with id '" +
            this.activity.get().getId() + "' is running.");
      }
      if (this.childContext.isPresent()) {
        throw new IllegalStateException("Attempting to instantiate a child context on a context that is not the topmost active context");
      }
      try {
        final Injector childServiceInjector = this.serviceInjector.forkInjector(serviceConfiguration);
        final ContextRuntime childContext = new ContextRuntime(childServiceInjector, contextConfiguration, Optional.of(this));
        this.childContext = Optional.of(childContext);
        return childContext;
      } catch (BindException e) {
        final Optional<String> parentID = this.getParentContext().isPresent() ?
            Optional.of(this.getParentContext().get().getIdentifier()) :
            Optional.<String>empty();
        throw new ContextClientCodeException(ContextClientCodeException.getIdentifier(contextConfiguration), parentID, "Unable to spawn context", e);
      }
    }
  }

  /**
   * Spawns a new context without services of its own.
   * <p/>
   * The new context will have a serviceInjector that is created by forking the one in this object. The
   * contextConfiguration is used to fork the contextInjector from that new serviceInjector.
   *
   * @param contextConfiguration the new context's context (local) Configuration.
   * @return a child context.
   * @throws ContextClientCodeException If the context can't be instantiate due to user code / configuration issues.
   * @throws IllegalStateException      If this method is called when there is either an activity or child context already
   *                                    present.
   */
  ContextRuntime spawnChildContext(final Configuration contextConfiguration) throws ContextClientCodeException {

    synchronized (this.contextLifeCycle) {
      if (this.activity.isPresent()) {
        throw new IllegalStateException("Attempting to to spawn a child context while an Activity with id '" +
            this.activity.get().getId() + "' is running.");
      }
      if (this.childContext.isPresent()) {
        throw new IllegalStateException("Attempting to spawn a child context on a context that is not the topmost active context");
      }
      final Injector childServiceInjector = this.serviceInjector.forkInjector();
      final ContextRuntime childContext = new ContextRuntime(childServiceInjector, contextConfiguration, Optional.of(this));
      this.childContext = Optional.of(childContext);
      return childContext;
    }
  }

  /**
   * Launches an Activity on this context.
   *
   * @param activityConfiguration the configuration to be used for the activity.
   * @throws ActivityClientCodeException If the Activity cannot be instantiated due to user code / configuration issues.
   * @throws IllegalStateException       If this method is called when there is either an activity or child context already
   *                                     present.
   */
  void startActivity(final Configuration activityConfiguration) throws ActivityClientCodeException {
    synchronized (this.contextLifeCycle) {
      if (this.activity.isPresent() && this.activity.get().hasEnded()) {
        // clean up state
        this.activity = Optional.empty();
      }

      if (this.activity.isPresent()) {
        throw new IllegalStateException("Attempting to start an Activity when an Activity with id '" +
            this.activity.get().getId() + "' is running.");
      }
      if (this.childContext.isPresent()) {
        throw new IllegalStateException("Attempting to start an Activity on a context that is not the topmost active context");
      }
      try {
        final Injector activityInjector = this.contextInjector.forkInjector(activityConfiguration);
        final ActivityRuntime activityRuntime = activityInjector.getInstance(ActivityRuntime.class);
        activityRuntime.initialize();
        activityRuntime.start();
        this.activity = Optional.of(activityRuntime);
        LOG.info("Started activity '" + activityRuntime.getActivityId());
      } catch (final BindException | InjectionException e) {
        throw new ActivityClientCodeException(ActivityClientCodeException.getActivityIdentifier(activityConfiguration),
            this.getIdentifier(),
            "Unable to instantiate the new activity",
            e);
      } catch (final Throwable t) {
        throw new ActivityClientCodeException(ActivityClientCodeException.getActivityIdentifier(activityConfiguration),
            this.getIdentifier(),
            "Unable to start the new activity",
            t);
      }
    }
  }

  /**
   * Close this context. If there is a child context, this recursively closes it before closing this context. If
   * there is an Activity currently running, that will be closed.
   */
  final void close() {
    synchronized (this.contextLifeCycle) {
      this.contextState = ReefServiceProtos.ContextStatusProto.State.DONE;
      if (this.activity.isPresent()) {
        LOG.log(Level.WARNING, "Shutting down an activity because the underlying context is being closed.");
        this.activity.get().close(null);
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
   * Deliver the given message to the Activity.
   * <p/>
   * Note that due to races, the activity might have already ended. In that case, we drop this call and leave a WARNING
   * in the log.
   *
   * @param message the suspend message to deliver or null if there is none.
   */
  void suspendActivity(final byte[] message) {
    synchronized (this.contextLifeCycle) {
      if (!this.activity.isPresent()) {
        LOG.log(Level.WARNING, "Received a suspend activity while there was no activity running. Ignoring.");
      } else {
        this.activity.get().suspend(message);
      }
    }
  }

  /**
   * Issue a close call to the Activity
   * <p/>
   * Note that due to races, the activity might have already ended. In that case, we drop this call and leave a WARNING
   * in the log.
   *
   * @param message the close  message to deliver or null if there is none.
   */
  void closeActivity(final byte[] message) {
    synchronized (this.contextLifeCycle) {
      if (!this.activity.isPresent()) {
        LOG.log(Level.WARNING, "Received a close activity while there was no activity running. Ignoring.");
      } else {
        this.activity.get().close(message);
      }
    }
  }

  /**
   * Deliver a message to the Activity
   * <p/>
   * Note that due to races, the activity might have already ended. In that case, we drop this call and leave a WARNING
   * in the log.
   *
   * @param message the close  message to deliver or null if there is none.
   */
  void deliverActivityMessage(final byte[] message) {
    synchronized (this.contextLifeCycle) {
      if (!this.activity.isPresent()) {
        LOG.log(Level.WARNING, "Received a activity message while there was no activity running. Ignoring.");
      } else {
        this.activity.get().deliver(message);
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
   * @return the state of the running Activity, if one is running.
   */
  Optional<ReefServiceProtos.ActivityStatusProto> getActivityStatus() {
    synchronized (this.contextLifeCycle) {
      if (this.activity.isPresent()) {
        if (this.activity.get().hasEnded()) {
          this.activity = Optional.empty();
          return Optional.empty();
        } else {
          final ReefServiceProtos.ActivityStatusProto activityStatusProto = this.activity.get().getStatusProto();
          if (activityStatusProto.getState() == ReefServiceProtos.State.RUNNING) {
            /* I'm only allowed to return RUNNING status here...
             * all other state pushed out to heartbeat */
            return Optional.of(activityStatusProto);
          }
          throw new IllegalStateException("Activity state must be RUNNING, but instead is in " + activityStatusProto.getState());
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
          ReefServiceProtos.ContextStatusProto.newBuilder().setContextId(this.getIdentifier()).setContextState(this.contextState);

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
