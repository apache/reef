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

import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.evaluator.context.ContextMessageHandler;
import org.apache.reef.evaluator.context.parameters.ContextIdentifier;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * Default handler for messages sent by the driver: Crash the context.
 */
@EvaluatorSide
public final class DefaultContextMessageHandler implements ContextMessageHandler {

  private final String contextID;

  @Inject
  DefaultContextMessageHandler(final @Parameter(ContextIdentifier.class) String contextID) {
    this.contextID = contextID;
  }

  @Override
  public void onNext(final byte[] message) {
    throw new IllegalStateException("No message handlers given for context " + this.contextID);
  }
}
