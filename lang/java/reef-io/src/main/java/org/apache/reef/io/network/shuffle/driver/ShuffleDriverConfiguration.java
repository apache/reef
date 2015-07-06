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
package org.apache.reef.io.network.shuffle.driver;

import org.apache.reef.driver.parameters.ServiceEvaluatorFailedHandlers;
import org.apache.reef.driver.parameters.ServiceTaskCompletedHandlers;
import org.apache.reef.driver.parameters.ServiceTaskFailedHandlers;
import org.apache.reef.driver.parameters.ServiceTaskRunningHandlers;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.wake.time.Clock;

/**
 *
 */
public final class ShuffleDriverConfiguration extends ConfigurationModuleBuilder {

  public static final String SHUFFLE_DRIVER_IDENTIFIER = "SHUFFLE_DRIVER_IDENTIFIER";

  public static ConfigurationModule CONF = new ShuffleDriverConfiguration()
      .bindSetEntry(ServiceTaskRunningHandlers.class, ShuffleDriverTaskRunningHandler.class)
      .bindSetEntry(ServiceTaskFailedHandlers.class, ShuffleDriverTaskFailedHandler.class)
      .bindSetEntry(ServiceTaskCompletedHandlers.class, ShuffleDriverTaskCompletedHandler.class)
      .bindSetEntry(ServiceEvaluatorFailedHandlers.class, ShuffleEvaluatorFailedHandler.class)
      .bindSetEntry(Clock.StartHandler.class, ShuffleDriverStartHandler.class)
      .bindSetEntry(Clock.StopHandler.class, ShuffleDriverStopHandler.class)
      .build();
}
