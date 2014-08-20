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

import com.microsoft.reef.driver.evaluator.EvaluatorDescriptor;
import com.microsoft.reef.runtime.common.driver.evaluator.EvaluatorManager;
import com.microsoft.reef.runtime.common.driver.evaluator.EvaluatorMessageDispatcher;
import com.microsoft.reef.runtime.common.utils.ExceptionCodec;
import com.microsoft.reef.util.Optional;
import com.microsoft.tang.InjectionFuture;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.formats.ConfigurationSerializer;
import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

import javax.inject.Inject;
import java.util.HashSet;
import java.util.Set;

/**
 * Helper class to generate EvaluatorContext instances. Used in ContextRepresenters.
 */
@ThreadSafe
final class ContextFactory {

  private final String evaluatorId;
  private final EvaluatorDescriptor evaluatorDescriptor;
  private final ConfigurationSerializer configurationSerializer;
  private final ExceptionCodec exceptionCodec;
  private final EvaluatorMessageDispatcher messageDispatcher;
  private final ContextControlHandler contextControlHandler;
  private final InjectionFuture<ContextRepresenters> contextRepresenters;


  @GuardedBy("this.priorIds")
  private final Set<String> priorIds = new HashSet<>();


  @Inject
  ContextFactory(final @Parameter(EvaluatorManager.EvaluatorIdentifier.class) String evaluatorId,
                 final @Parameter(EvaluatorManager.EvaluatorDescriptorName.class) EvaluatorDescriptor evaluatorDescriptor,
                 final ConfigurationSerializer configurationSerializer,
                 final ExceptionCodec exceptionCodec,
                 final EvaluatorMessageDispatcher messageDispatcher,
                 final ContextControlHandler contextControlHandler,
                 final InjectionFuture<ContextRepresenters> contextRepresenters) {
    this.evaluatorId = evaluatorId;
    this.evaluatorDescriptor = evaluatorDescriptor;
    this.configurationSerializer = configurationSerializer;
    this.exceptionCodec = exceptionCodec;
    this.messageDispatcher = messageDispatcher;
    this.contextControlHandler = contextControlHandler;
    this.contextRepresenters = contextRepresenters;
  }

  /**
   * Instantiate a new Context representer with the given id and parent id.
   *
   * @param contextId
   * @param parentID
   * @return a new Context representer with the given id and parent id.
   */
  public final EvaluatorContext newContext(final String contextId, final Optional<String> parentID) {
    synchronized (this.priorIds) {
      if (this.priorIds.contains(contextId)) {
        throw new IllegalStateException("Creating second EvaluatorContext instance for id " + contextId);
      }
      this.priorIds.add(contextId);
    }
    return new EvaluatorContext(contextId,
        this.evaluatorId,
        this.evaluatorDescriptor,
        parentID,
        this.configurationSerializer,
        this.contextControlHandler,
        this.messageDispatcher,
        this.exceptionCodec,
        this.contextRepresenters.get());
  }
}

