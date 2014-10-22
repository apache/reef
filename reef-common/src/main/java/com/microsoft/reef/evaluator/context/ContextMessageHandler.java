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
package com.microsoft.reef.evaluator.context;

import com.microsoft.reef.annotations.Optional;
import com.microsoft.reef.annotations.audience.EvaluatorSide;
import com.microsoft.reef.annotations.audience.Public;
import com.microsoft.wake.EventHandler;

/**
 * Implement this interface to receive messages from the driver in a context.
 */
@EvaluatorSide
@Public
@Optional
public interface ContextMessageHandler extends EventHandler<byte[]> {

  /**
   * @param message sent by the driver to this context
   */
  @Override
  public void onNext(final byte[] message);
}
