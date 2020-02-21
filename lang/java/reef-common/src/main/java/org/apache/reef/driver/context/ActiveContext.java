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
package org.apache.reef.driver.context;

import org.apache.reef.annotations.Provided;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Public;
import org.apache.reef.driver.ContextSubmittable;
import org.apache.reef.driver.TaskSubmittable;
import org.apache.reef.io.naming.Identifiable;
import org.apache.reef.tang.Configuration;

/**
 * Represents an active context on an Evaluator.
 * <p>
 * A context consists of two configurations:
 * <ol>
 * <li>ContextConfiguration: Its visibility is limited to the context itself and tasks spawned from it.</li>
 * <li>ServiceConfiguration: This is "inherited" by child context spawned.</li>
 * </ol>
 * <p>
 * Contexts have identifiers. A context is instantiated on a single Evaluator. Contexts are either created on an
 * AllocatedEvaluator (for what is called the "root Context") or by forming sub-Contexts.
 * <p>
 * Contexts form a stack. Only the topmost context is active. Child Contexts or Tasks can be submitted to the
 * active Context. Contexts can be closed, in which case their parent becomes active.
 * In the case of the root context, closing is equivalent to releasing the Evaluator. A child context "sees" all
 * Configuration in its parent Contexts.
 */
@Public
@DriverSide
@Provided
public interface ActiveContext extends Identifiable, AutoCloseable, ContextBase, TaskSubmittable, ContextSubmittable {

  @Override
  void close();

  @Override
  void submitTask(Configuration taskConf);

  @Override
  void submitContext(Configuration contextConfiguration);

  @Override
  void submitContextAndService(Configuration contextConfiguration, Configuration serviceConfiguration);

  /**
   * Send the active context the message, which will be delivered to all registered
   * {@link org.apache.reef.evaluator.context.ContextMessageHandler}, for this context.
   *
   * @param message The message to be sent.
   */
  void sendMessage(byte[] message);

}
