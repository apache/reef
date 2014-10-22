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
package com.microsoft.reef.runtime.common.driver.context;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.driver.context.FailedContext;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.runtime.common.driver.evaluator.EvaluatorMessageDispatcher;
import com.microsoft.reef.util.Optional;
import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver-Side representation of all contexts on an Evaluator.
 */
@ThreadSafe
@DriverSide
@Private
public final class ContextRepresenters {
  private static final Logger LOG = Logger.getLogger(ContextRepresenters.class.getName());

  private final EvaluatorMessageDispatcher messageDispatcher;
  private final ContextFactory contextFactory;

  // Mutable fields
  @GuardedBy("this")
  private final List<EvaluatorContext> contextStack = new ArrayList<>();
  @GuardedBy("this")
  private final Set<String> contextIds = new HashSet<>();

  @Inject
  private ContextRepresenters(final EvaluatorMessageDispatcher messageDispatcher,
                              final ContextFactory contextFactory) {
    this.messageDispatcher = messageDispatcher;
    this.contextFactory = contextFactory;
  }

  /**
   * Fetch the context with the given ID.
   *
   * @param contextId
   * @return
   */
  public synchronized EvaluatorContext getContext(final String contextId) {
    for (final EvaluatorContext context : this.contextStack) {
      if (context.getId().equals(contextId)) return context;
    }
    throw new RuntimeException("Unknown evaluator context " + contextId);
  }

  /**
   * Create the failed contexts for a FailedEvaluator event.
   *
   * @return
   */
  public synchronized List<FailedContext> getFailedContextsForEvaluatorFailure() {
    final List<FailedContext> failedContextList = new ArrayList<>();
    final List<EvaluatorContext> activeContexts = new ArrayList<>(this.contextStack);
    Collections.reverse(activeContexts);

    for (final EvaluatorContext context : activeContexts) {
      failedContextList.add(context.getFailedContextForEvaluatorFailure());
    }
    return failedContextList;
  }

  /**
   * Process heartbeats from the contexts on an Evaluator.
   *
   * @param contextStatusProto
   * @param notifyClientOnNewActiveContext
   */
  public synchronized void onContextStatusMessages(final Iterable<ReefServiceProtos.ContextStatusProto> contextStatusProtos,
                                                   final boolean notifyClientOnNewActiveContext) {
    for (final ReefServiceProtos.ContextStatusProto contextStatusProto : contextStatusProtos) {
      this.onContextStatusMessage(contextStatusProto, notifyClientOnNewActiveContext);
    }
  }


  /**
   * Process a heartbeat from a context
   *
   * @param contextStatusProto
   * @param notifyClientOnNewActiveContext
   */
  private synchronized void onContextStatusMessage(final ReefServiceProtos.ContextStatusProto contextStatusProto,
                                                   final boolean notifyClientOnNewActiveContext) {

    LOG.log(Level.FINER, "Processing context status message for context {0}", contextStatusProto.getContextId());
    switch (contextStatusProto.getContextState()) {
      case READY:
        this.onContextReady(contextStatusProto, notifyClientOnNewActiveContext);
        break;
      case FAIL:
        this.onContextFailed(contextStatusProto);
        break;
      case DONE:
        this.onContextDone(contextStatusProto);
        break;
      default:
        this.onUnknownContextStatus(contextStatusProto);
        break;
    }
    LOG.log(Level.FINER, "Done processing context status message for context {0}", contextStatusProto.getContextId());

  }


  private synchronized void onUnknownContextStatus(final ReefServiceProtos.ContextStatusProto contextStatusProto) {
    LOG.log(Level.WARNING, "Received unexpected context status: {0}", contextStatusProto);
    throw new RuntimeException("Received unexpected context status: " + contextStatusProto.getContextState());
  }

  private synchronized void onContextFailed(final ReefServiceProtos.ContextStatusProto contextStatusProto) {
    assert (ReefServiceProtos.ContextStatusProto.State.FAIL == contextStatusProto.getContextState());
    final String contextID = contextStatusProto.getContextId();
    LOG.log(Level.FINE, "Context {0} failed", contextID);
    // It could have failed right away.
    if (this.isUnknownContextId(contextID)) {
      this.onNewContext(contextStatusProto, false);
    }
    final EvaluatorContext context = getContext(contextID);
    this.removeContext(context);
    this.messageDispatcher.onContextFailed(context.getFailedContext(contextStatusProto));
  }

  private synchronized void onContextDone(final ReefServiceProtos.ContextStatusProto contextStatusProto) {
    assert (ReefServiceProtos.ContextStatusProto.State.DONE == contextStatusProto.getContextState());
    final String contextID = contextStatusProto.getContextId();
    if (isUnknownContextId(contextID)) {
      throw new RuntimeException("Received DONE for context " + contextID + " which is unknown.");
    } else {
      LOG.log(Level.FINE, "Context {0} is DONE.", contextID);
      final EvaluatorContext context = getContext(contextID);
      removeContext(context);

      if (context.isRootContext()) {
        LOG.log(Level.FINE, "Root context {0} closed. Evaluator closed will trigger final shutdown.", contextID);
      } else {
        final EvaluatorContext parentContext = this.getContext(context.getParentId().get());
        this.messageDispatcher.onContextClose(context.getClosedContext(parentContext));
      }
    }
  }

  /**
   * Process a message with status READY from a context.
   *
   * @param contextStatusProto
   * @param notifyClientOnNewActiveContext whether or not to inform the application when this in fact refers to a new
   *                                       context.
   */
  private synchronized void onContextReady(final ReefServiceProtos.ContextStatusProto contextStatusProto,
                                           final boolean notifyClientOnNewActiveContext) {
    assert (ReefServiceProtos.ContextStatusProto.State.READY == contextStatusProto.getContextState());
    final String contextID = contextStatusProto.getContextId();
    // This could be the first message we get from that context
    if (this.isUnknownContextId(contextID)) {
      this.onNewContext(contextStatusProto, notifyClientOnNewActiveContext);
    }

    // Dispatch the messages to the application, if there are any.
    for (final ReefServiceProtos.ContextStatusProto.ContextMessageProto contextMessageProto : contextStatusProto.getContextMessageList()) {
      final byte[] theMessage = contextMessageProto.getMessage().toByteArray();
      final String sourceID = contextMessageProto.getSourceId();
      this.messageDispatcher.onContextMessage(new ContextMessageImpl(theMessage, contextID, sourceID));
    }

  }

  /**
   * Create and add a new context representer.
   *
   * @param contextStatusProto             the message to create the context from
   * @param notifyClientOnNewActiveContext whether or not to fire an event to the user.
   */
  private synchronized void onNewContext(final ReefServiceProtos.ContextStatusProto contextStatusProto,
                                         final boolean notifyClientOnNewActiveContext) {
    final String contextID = contextStatusProto.getContextId();
    LOG.log(Level.FINE, "Adding new context {0}.", contextID);

    final Optional<String> parentID = contextStatusProto.hasParentId() ?
        Optional.of(contextStatusProto.getParentId()) : Optional.<String>empty();
    final EvaluatorContext context = contextFactory.newContext(contextID, parentID);
    this.addContext(context);
    if(contextStatusProto.getRecovery()){
      // when we get a recovered active context, always notify application
      this.messageDispatcher.OnDriverRestartContextActive(context);
    }
    else{
      if (notifyClientOnNewActiveContext) {
        this.messageDispatcher.onContextActive(context);
      }
    }
  }

  /**
   * Add the given context to the data structures.
   *
   * @param context
   */
  private synchronized void addContext(final EvaluatorContext context) {
    this.contextStack.add(context);
    this.contextIds.add(context.getId());
  }

  /**
   * Remove the given context from the data structures.
   *
   * @param context
   */
  private synchronized void removeContext(final EvaluatorContext context) {
    this.contextStack.remove(context);
    this.contextIds.remove(context.getId());
  }

  /**
   * @param contextId
   * @return true if the given context id is unknown so far.
   */
  private synchronized boolean isUnknownContextId(final String contextId) {
    return !this.contextIds.contains(contextId);
  }

}
