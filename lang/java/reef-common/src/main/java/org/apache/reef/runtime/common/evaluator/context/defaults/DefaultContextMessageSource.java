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
package org.apache.reef.runtime.common.evaluator.context.defaults;

import org.apache.reef.annotations.Provided;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.evaluator.context.ContextMessage;
import org.apache.reef.evaluator.context.ContextMessageSource;
import org.apache.reef.util.Optional;

import javax.inject.Inject;

/**
 * Default ContextMessageSource: return nothing.
 */
@EvaluatorSide
@Provided
public final class DefaultContextMessageSource implements ContextMessageSource {

  @Inject
  public DefaultContextMessageSource() {
  }

  @Override
  public Optional<ContextMessage> getMessage() {
    return Optional.empty();
  }
}
