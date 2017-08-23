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
package org.apache.reef.runtime.common.driver.defaults;

import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.evaluator.PreemptedEvaluator;
import org.apache.reef.driver.parameters.EvaluatorFailedHandlers;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.Set;

/**
 * Default event handler used for PreemptedEvaluator: It invokes the registered FailedEvaluator handlers.
 */
public final class DefaultEvaluatorPreemptionHandler implements EventHandler<PreemptedEvaluator> {
  private final Set<EventHandler<FailedEvaluator>> evaluatorFailedHandlers;

  @Inject
  public DefaultEvaluatorPreemptionHandler(@Parameter(EvaluatorFailedHandlers.class)
                                           final Set<EventHandler<FailedEvaluator>> evaluatorFailedHandlers) {
    this.evaluatorFailedHandlers = evaluatorFailedHandlers;
  }

  @Override
  public void onNext(final PreemptedEvaluator preemptedEvaluator) {
    for (final EventHandler<FailedEvaluator> handler : evaluatorFailedHandlers) {
      handler.onNext(preemptedEvaluator);
    }
  }
}
