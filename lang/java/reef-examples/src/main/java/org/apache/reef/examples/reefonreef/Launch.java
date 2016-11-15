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
package org.apache.reef.examples.reefonreef;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.EnvironmentUtils;

import java.util.logging.Level;
import java.util.logging.Logger;

/** The Client for running REEF-on-REEF application on YARN. */
public final class Launch {

  private static final Logger LOG = Logger.getLogger(Launch.class.getName());

  /**
   * Number of milliseconds to wait for the job to complete.
   * Setting to 100 sec because running on RM HA clusters take around
   * 50 seconds to set the job to running.
   */
  private static final int JOB_TIMEOUT = 100000; // 100 sec.

  private static final Configuration RUNTIME_CONFIG = YarnClientConfiguration.CONF.build();

  private static final Configuration DRIVER_CONFIG = DriverConfiguration.CONF
      .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(ReefOnReefDriver.class))
      .set(DriverConfiguration.DRIVER_IDENTIFIER, "REEF-on-REEF:host")
      .set(DriverConfiguration.ON_DRIVER_STARTED, ReefOnReefDriver.class)
      .build();

  /**
   * Start REEF-on-REEF job on YARN.
   * @param args command line parameters (not used).
   * @throws InjectionException configuration error.
   */
  public static void main(final String[] args) throws InjectionException {
    final LauncherStatus status = DriverLauncher.getLauncher(RUNTIME_CONFIG).run(DRIVER_CONFIG, JOB_TIMEOUT);
    LOG.log(Level.INFO, "REEF-on-REEF host job completed: {0}", status);
  }

  /** Empty private constructor to prohibit instantiation of utility class. */
  private Launch() { }
}
