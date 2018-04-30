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
package org.apache.reef.runtime.yarn.driver;

import org.apache.reef.annotations.Provided;
import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.annotations.audience.Public;
import org.apache.reef.driver.restart.DriverRuntimeRestartManager;
import org.apache.reef.runtime.common.driver.DriverRuntimeRestartConfiguration;
import org.apache.reef.runtime.common.driver.EvaluatorPreserver;
import org.apache.reef.runtime.yarn.driver.parameters.YarnEvaluatorPreserver;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalImpl;

/**
 * Use this ConfigurationModule to configure YARN-specific Restart options for the driver.
 * <p>
 */
@ClientSide
@Public
@Provided
@Unstable
public final class YarnDriverRestartConfiguration extends ConfigurationModuleBuilder {
  /**
   * The Evaluator Preserver implementation used for YARN. Defaults to DFSEvaluatorPreserver.
   */
  public static final OptionalImpl<EvaluatorPreserver> EVALUATOR_PRESERVER = new OptionalImpl<>();

  /**
   * This event is fired in place of the ON_DRIVER_STARTED when the Driver is in fact restarted after failure.
   */
  public static final ConfigurationModule CONF = new YarnDriverRestartConfiguration()
      .bindNamedParameter(YarnEvaluatorPreserver.class, EVALUATOR_PRESERVER)
      .bindImplementation(DriverRuntimeRestartManager.class, YarnDriverRuntimeRestartManager.class)
      .merge(DriverRuntimeRestartConfiguration.CONF)
      .build();
}
