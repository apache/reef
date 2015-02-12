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
package org.apache.reef.examples.nggroup.matmul;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.EnvironmentUtils;

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
    } catch (final InjectionException ex) {
      LOG.log(Level.SEVERE, "Fatal Exception during job", ex);
      return LauncherStatus.FAILED(ex);
    }
  }

  /**
   * Start MatMult REEF job. Runs method runMatMultReef().
   *
   * @param args command line parameters.
   * @throws InjectionException configuration error.
   */
  public static void main(final String[] args) throws InjectionException {
    final Configuration runtimeConfiguration = LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, 6)
        .build();
    final LauncherStatus status = run(runtimeConfiguration);
    LOG.log(Level.INFO, "Matrix multiply returned: {0}", status);
  }
}
