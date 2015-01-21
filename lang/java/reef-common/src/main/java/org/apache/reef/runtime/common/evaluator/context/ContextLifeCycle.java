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

import org.apache.reef.evaluator.context.ContextMessageSource;
import org.apache.reef.evaluator.context.events.ContextStart;
import org.apache.reef.evaluator.context.events.ContextStop;
import org.apache.reef.evaluator.context.parameters.*;
import org.apache.reef.runtime.common.utils.BroadCastEventHandler;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * This class is used to trigger all the context life-cycle dependent events.
 */
final class ContextLifeCycle {

  private final String identifier;
  private final Set<EventHandler<ContextStart>> contextStartHandlers;
  private final Set<EventHandler<ContextStop>> contextStopHandlers;
  private final Set<ContextMessageSource> contextMessageSources;
  private final EventHandler<byte[]> contextMessageHandler;

  @Inject
  ContextLifeCycle(final @Parameter(ContextIdentifier.class) String identifier,
                   final @Parameter(ContextMessageHandlers.class) Set<EventHandler<byte[]>> contextMessageHandlers,
                   final @Parameter(ContextStartHandlers.class) Set<EventHandler<ContextStart>> contextStartHandlers,
                   final @Parameter(ContextStopHandlers.class) Set<EventHandler<ContextStop>> contextStopHandlers,
                   final @Parameter(ContextMessageSources.class) Set<ContextMessageSource> contextMessageSources) {
    this.identifier = identifier;
    this.contextStartHandlers = contextStartHandlers;
    this.contextStopHandlers = contextStopHandlers;
    this.contextMessageSources = contextMessageSources;
    this.contextMessageHandler = new BroadCastEventHandler<>(contextMessageHandlers);
  }

  /**
   * Fires ContextStart to all registered event handlers.
   */
  final void start() {
    final ContextStart contextStart = new ContextStartImpl(this.identifier);
    for (final EventHandler<ContextStart> startHandler : this.contextStartHandlers) {
      startHandler.onNext(contextStart);
    }
  }

  /**
   * Fires ContextStop to all registered event handlers.
   */
  final void close() {
    final ContextStop contextStop = new ContextStopImpl(this.identifier);
    for (final EventHandler<ContextStop> stopHandler : this.contextStopHandlers) {
      stopHandler.onNext(contextStop);
    }
  }

  /**
   * Deliver the driver message to the context message handler
   *
   * @param message sent by the driver
   */
  final void handleContextMessage(final byte[] message) {
    this.contextMessageHandler.onNext(message);
  }

  /**
   * @return (a shallow copy of) the set of ContextMessageSources configured.
   */
  final Set<ContextMessageSource> getContextMessageSources() {
    return Collections.unmodifiableSet(new LinkedHashSet<>(this.contextMessageSources));
  }

  final String getIdentifier() {
    return this.identifier;
  }
}
