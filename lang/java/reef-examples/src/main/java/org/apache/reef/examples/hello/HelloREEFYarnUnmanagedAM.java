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
package org.apache.reef.examples.hello;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.runtime.common.REEFEnvironment;
import org.apache.reef.runtime.yarn.client.unmanaged.UnmanagedAmYarnClientConfiguration;
import org.apache.reef.runtime.yarn.client.unmanaged.UnmanagedAmYarnDriverConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.util.ThreadLogger;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Client for running HelloREEF on YARN in unmanaged AM mode.
 */
public final class HelloREEFYarnUnmanagedAM {

  private static final Logger LOG = Logger.getLogger(HelloREEFYarnUnmanagedAM.class.getName());

  private static final String DRIVER_ROOT_PATH = ".";
  private static final String JAR_PATH = EnvironmentUtils.getClassLocation(HelloREEFYarnUnmanagedAM.class);

  private static final Configuration RUNTIME_CONFIG = UnmanagedAmYarnClientConfiguration.CONF
      .set(UnmanagedAmYarnClientConfiguration.ROOT_FOLDER, DRIVER_ROOT_PATH)
      .build();

  private static final Configuration DRIVER_CONFIG = DriverConfiguration.CONF
      .set(DriverConfiguration.GLOBAL_LIBRARIES, JAR_PATH)
      .set(DriverConfiguration.DRIVER_IDENTIFIER, "HelloReef_UnmanagedAm")
      .set(DriverConfiguration.ON_DRIVER_STARTED, HelloDriver.StartHandler.class)
      .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, HelloDriver.EvaluatorAllocatedHandler.class)
      .build();

  /**
   * Start Hello REEF job with Unmanaged Driver running locally in the same process.
   * @param args command line parameters. Not used.
   * @throws InjectionException configuration error.
   */
  public static void main(final String[] args) throws InjectionException {

    LOG.log(Level.FINE, "Launching Unmanaged AM: {0}", JAR_PATH);

    try (final DriverLauncher client = DriverLauncher.getLauncher(RUNTIME_CONFIG)) {

      final String appId = client.submit(DRIVER_CONFIG, 10000);
      LOG.log(Level.INFO, "Job submitted: {0}", appId);

      final Configuration yarnAmConfig = UnmanagedAmYarnDriverConfiguration.CONF
          .set(UnmanagedAmYarnDriverConfiguration.JOB_IDENTIFIER, appId)
          .set(UnmanagedAmYarnDriverConfiguration.JOB_SUBMISSION_DIRECTORY, DRIVER_ROOT_PATH)
          .build();

      try (final REEFEnvironment reef = REEFEnvironment.fromConfiguration(yarnAmConfig, DRIVER_CONFIG)) {
        reef.run();
        final ReefServiceProtos.JobStatusProto status = reef.getLastStatus();
        LOG.log(Level.INFO, "REEF job {0} completed: state {1}", new Object[] {appId, status.getState()});
      }
    }

    ThreadLogger.logThreads(LOG, Level.FINEST, "Threads running after DriverLauncher.close():");
    System.exit(0); // TODO[REEF-1715]: Should be able to exit cleanly at the end of main()
  }

  /** Empty private constructor to prohibit instantiation of utility class. */
  private HelloREEFYarnUnmanagedAM() { }
}
