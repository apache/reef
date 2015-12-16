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
package org.apache.reef.runtime.common.driver.context;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.restart.DriverRestartManager;
import org.apache.reef.driver.restart.EvaluatorRestartState;
import org.apache.reef.runtime.common.driver.evaluator.EvaluatorMessageDispatcher;
import org.apache.reef.runtime.common.driver.evaluator.pojos.ContextMessagePOJO;
import org.apache.reef.runtime.common.driver.evaluator.pojos.ContextState;
import org.apache.reef.runtime.common.driver.evaluator.pojos.ContextStatusPOJO;
import org.apache.reef.util.Optional;

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

  private final DriverRestartManager driverRestartManager;

  @Inject
  private ContextRepresenters(final EvaluatorMessageDispatcher messageDispatcher,
                              final ContextFactory contextFactory,
                              final DriverRestartManager driverRestartManager) {
    this.messageDispatcher = messageDispatcher;
    this.contextFactory = contextFactory;
    this.driverRestartManager = driverRestartManager;
  }

  /**
   * Fetch the context with the given ID.
   *
   * @param contextId
   * @return
   */
  public synchronized EvaluatorContext getContext(final String contextId) {
    for (final EvaluatorContext context : this.contextStack) {
      if (context.getId().equals(contextId)) {
        return context;
      }
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
   * @param contextStatusPOJOs
   * @param notifyClientOnNewActiveContext
   */
  public synchronized void onContextStatusMessages(final Iterable<ContextStatusPOJO>
                                                       contextStatusPOJOs,
                                                   final boolean notifyClientOnNewActiveContext) {
    for (final ContextStatusPOJO contextStatus : contextStatusPOJOs) {
      this.onContextStatusMessage(contextStatus, notifyClientOnNewActiveContext);
    }
  }


  /**
   * Process a heartbeat from a context.
   *
   * @param contextStatus
   * @param notifyClientOnNewActiveContext
   */
  private synchronized void onContextStatusMessage(final ContextStatusPOJO contextStatus,
                                                   final boolean notifyClientOnNewActiveContext) {

    LOG.log(Level.FINER, "Processing context status message for context {0}", contextStatus.getContextId());
    switch (contextStatus.getContextState()) {
    case READY:
      this.onContextReady(contextStatus, notifyClientOnNewActiveContext);
      break;
    case FAIL:
      this.onContextFailed(contextStatus);
      break;
    case DONE:
      this.onContextDone(contextStatus);
      break;
    default:
      this.onUnknownContextStatus(contextStatus);
      break;
    }
    LOG.log(Level.FINER, "Done processing context status message for context {0}", contextStatus.getContextId());

  }


  private synchronized void onUnknownContextStatus(final ContextStatusPOJO contextStatus) {
    LOG.log(Level.WARNING, "Received unexpected context status: {0}", contextStatus);
    throw new RuntimeException("Received unexpected context status: " + contextStatus.getContextState());
  }

  private synchronized void onContextFailed(final ContextStatusPOJO contextStatus) {
    assert ContextState.FAIL == contextStatus.getContextState();
    final String contextID = contextStatus.getContextId();
    LOG.log(Level.FINE, "Context {0} failed", contextID);
    // It could have failed right away.
    if (this.isUnknownContextId(contextID)) {
      this.onNewContext(contextStatus, false);
    }
    final EvaluatorContext context = getContext(contextID);
    this.removeContext(context);
    this.messageDispatcher.onContextFailed(context.getFailedContext(contextStatus));
  }

  private synchronized void onContextDone(final ContextStatusPOJO contextStatus) {
    assert ContextState.DONE == contextStatus.getContextState();
    final String contextID = contextStatus.getContextId();
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
   * @param contextStatus
   * @param notifyClientOnNewActiveContext whether or not to inform the application when this in fact refers to a new
   *                                       context.
   */
  private synchronized void onContextReady(final ContextStatusPOJO contextStatus,
                                           final boolean notifyClientOnNewActiveContext) {
    assert ContextState.READY == contextStatus.getContextState();
    final String contextID = contextStatus.getContextId();
    // This could be the first message we get from that context
    if (this.isUnknownContextId(contextID)) {
      this.onNewContext(contextStatus, notifyClientOnNewActiveContext);
    }

    // Dispatch the messages to the application, if there are any.
    for (final ContextMessagePOJO
             contextMessage : contextStatus.getContextMessageList()) {
      final byte[] theMessage = contextMessage.getMessage();
      final String sourceID = contextMessage.getSourceId();
      final long sequenceNumber = contextMessage.getSequenceNumber();
      this.messageDispatcher.onContextMessage(new ContextMessageImpl(theMessage, contextID, sourceID, sequenceNumber));
    }

  }

  /**
   * Create and add a new context representer.
   *
   * @param contextStatus             the message to create the context from
   * @param notifyClientOnNewActiveContext whether or not to fire an event to the user.
   */
  private synchronized void onNewContext(final ContextStatusPOJO contextStatus,
                                         final boolean notifyClientOnNewActiveContext) {
    final String contextID = contextStatus.getContextId();
    LOG.log(Level.FINE, "Adding new context {0}.", contextID);

    final Optional<String> parentID = contextStatus.hasParentId() ?
        Optional.of(contextStatus.getParentId()) : Optional.<String>empty();
    final EvaluatorContext context = contextFactory.newContext(contextID, parentID);
    this.addContext(context);
    if (notifyClientOnNewActiveContext) {
      if (driverRestartManager.getEvaluatorRestartState(context.getEvaluatorId())
          == EvaluatorRestartState.REREGISTERED) {
        // if restart, advance restart state and all the restart context active handlers.
        driverRestartManager.setEvaluatorProcessed(context.getEvaluatorId());
        this.messageDispatcher.onDriverRestartContextActive(context);
      }

      this.messageDispatcher.onContextActive(context);
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
