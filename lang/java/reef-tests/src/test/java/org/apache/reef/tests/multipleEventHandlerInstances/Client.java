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
package org.apache.reef.tests.multipleEventHandlerInstances;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.EnvironmentUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.logging.Logger;

/**
 *
 */
public class Client {
  private static final Logger LOG = Logger.getLogger(Client.class.getName());

  /**
   * Number of milliseconds to wait for the job to complete.
   */
  private static final int JOB_TIMEOUT = 300000; // 300 sec.

  public static LauncherStatus runReefJob(final Configuration runtimeConf, final int timeOut)
      throws BindException, InjectionException {

    final Configuration driverConf = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(StartHandler.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "MultipleHandlerInstances")
        .set(DriverConfiguration.ON_DRIVER_STARTED, StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, AllocatedEvaluatorHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, ActiveContextHandler.class)
        .set(DriverConfiguration.ON_TASK_RUNNING, RunningTaskHandler.class)
        .set(DriverConfiguration.ON_TASK_COMPLETED, CompletedTaskHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_CLOSED, ClosedContextHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_COMPLETED, CompletedEvaluatorHandler.class)
        .build();

    return DriverLauncher.getLauncher(runtimeConf).run(driverConf, timeOut);
  }

  /**
   * @throws BindException      configuration error.
   * @throws InjectionException configuration error.
   */
  @Test
  public void testMultipleInstances() throws BindException, InjectionException {
    final Configuration runtimeConfiguration = LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, 2)
        .build();
    final LauncherStatus status = runReefJob(runtimeConfiguration, JOB_TIMEOUT);
    Assert.assertTrue("Reef Job MultipleHandlerInstances failed: " + status, status.isSuccess());
  }
}
