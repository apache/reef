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
package com.microsoft.reef.runtime.common.driver.contexts.defaults;

import com.microsoft.reef.driver.context.ContextConfigurationOptions;
import com.microsoft.reef.evaluator.context.ContextMessageHandler;
import com.microsoft.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * Default handler for ContextStart
 */
public final class DefaultContextMessageHandler implements ContextMessageHandler {

  private final String contextID;

  @Inject
  DefaultContextMessageHandler(final @Parameter(ContextConfigurationOptions.ContextIdentifier.class) String contextID) {
    this.contextID = contextID;
  }

  @Override
  public void onNext(final byte[] message) {
    throw new IllegalStateException("No message handlers given for context " + this.contextID);
  }
}
