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
package com.microsoft.reef.driver.context;

import com.microsoft.reef.annotations.Provided;
import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Public;
import com.microsoft.reef.driver.ActivitySubmittable;
import com.microsoft.reef.driver.ContextSubmittable;
import com.microsoft.reef.io.naming.Identifiable;
import com.microsoft.tang.Configuration;

/**
 * Represents an active context on an Evaluator.
 * <p/>
 * A context consists of twp configurations:
 * <ol>
 * <li>ContextConfiguration: Its visibility is limited to the context itself and activities spawned from it.</li>
 * <li>ServiceConfiguration: This is "inherited" by child context spawned.</li>
 * </ol>
 * <p/>
 * Contexts have identifiers. A context is instantiated on a single Evaluator. Contexts are either created on an
 * AllocatedEvaluator (for the root context) or by forming sub-Contexts.
 * <p/>
 * Contexts form a stack. Only the topmost context is active. Child context or activities can be submitted to the
 * active context. Contexts can be closed, in which case their parent becomes active.
 * In the case of the root context, closing is equivalent to releasing the Evaluator. A child context “sees�? all
 * Configuration in its parent Contexts. Activities are submitted to and executed in a context.
 */
@Public
@DriverSide
@Provided
public interface ActiveContext extends Identifiable, AutoCloseable, ContextBase, ActivitySubmittable, ContextSubmittable {

  @Override
  public void close();

  @Override
  public void submitActivity(final Configuration activityConf);

  @Override
  public void submitContext(final Configuration contextConfiguration);

  @Override
  public void submitContextAndService(final Configuration contextConfiguration, final Configuration serviceConfiguration);

  /**
   * Send the active context the message, which will be delivered to all registered
   * {@link com.microsoft.reef.evaluator.context.ContextMessageHandler}, for this context.
   * @param message
   */
  public void sendMessage(final byte[] message);

}
