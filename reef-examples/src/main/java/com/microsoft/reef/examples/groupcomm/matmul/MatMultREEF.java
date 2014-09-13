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
package com.microsoft.reef.examples.groupcomm.matmul;

import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.DriverLauncher;
import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Client for the Matrix Multiplication REEF example
 * using REEF group communication operators.
 */
public final class MatMultREEF {
  private static final Logger LOG = Logger.getLogger(MatMultREEF.class.getName());

  public static LauncherStatus run(final Configuration runtimeConfiguration) {
    try {
      final Configuration driverConfiguration = DriverConfiguration.CONF
          .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(MatMultDriver.class))
          .set(DriverConfiguration.ON_DRIVER_STARTED, MatMultDriver.StartHandler.class)
          .set(DriverConfiguration.DRIVER_IDENTIFIER, "MatrixMultiply")
          .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, MatMultDriver.AllocatedEvaluatorHandler.class)
          .set(DriverConfiguration.ON_TASK_RUNNING, MatMultDriver.RunningTaskHandler.class)
          .set(DriverConfiguration.ON_TASK_COMPLETED, MatMultDriver.CompletedTaskHandler.class)
          .set(DriverConfiguration.ON_CONTEXT_ACTIVE, MatMultDriver.ActiveContextHandler.class)
          .build();
      return DriverLauncher.getLauncher(runtimeConfiguration).run(driverConfiguration, 100000);
    } catch (final BindException | InjectionException ex) {
      LOG.log(Level.SEVERE, "Fatal Exception during job", ex);
      return LauncherStatus.FAILED(ex);
    }
  }

  /**
   * Start MatMult REEF job. Runs method runMatMultReef().
   *
   * @param args command line parameters.
   * @throws BindException      configuration error.
   * @throws InjectionException configuration error.
   */
  public static void main(final String[] args) throws BindException, InjectionException {
    final Configuration runtimeConfiguration = LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, 6)
        .build();
    final LauncherStatus status = run(runtimeConfiguration);
    LOG.log(Level.INFO, "Matrix multiply returned: {0}", status);
  }
}
