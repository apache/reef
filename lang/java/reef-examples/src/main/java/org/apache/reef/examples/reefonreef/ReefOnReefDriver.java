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
import org.apache.reef.examples.hello.HelloDriver;
import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.runtime.common.REEFEnvironment;
import org.apache.reef.runtime.common.launch.parameters.LaunchID;
import org.apache.reef.runtime.yarn.client.unmanaged.UnmanagedAmYarnClientConfiguration;
import org.apache.reef.runtime.yarn.client.unmanaged.UnmanagedAmYarnDriverConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.util.ThreadLogger;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Driver code for the Hello REEF Application.
 */
final class ReefOnReefDriver implements EventHandler<StartTime> {

  private static final Logger LOG = Logger.getLogger(ReefOnReefDriver.class.getName());

  private static final String DRIVER_ROOT_PATH = ".";
  private static final String JAR_PATH = EnvironmentUtils.getClassLocation(ReefOnReefDriver.class);

  private static final Configuration RUNTIME_CONFIG = UnmanagedAmYarnClientConfiguration.CONF
      .set(UnmanagedAmYarnClientConfiguration.ROOT_FOLDER, DRIVER_ROOT_PATH)
      .build();

  private static final Configuration DRIVER_CONFIG = DriverConfiguration.CONF
      .set(DriverConfiguration.DRIVER_IDENTIFIER, "REEF-on-REEF:hello")
      .set(DriverConfiguration.GLOBAL_LIBRARIES, JAR_PATH)
      .set(DriverConfiguration.ON_DRIVER_STARTED, HelloDriver.StartHandler.class)
      .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, HelloDriver.EvaluatorAllocatedHandler.class)
      .build();

  private final String hostApplicationId;

  @Inject
  private ReefOnReefDriver(@Parameter(LaunchID.class) final String applicationId) {
    LOG.log(Level.FINE, "Instantiated ReefOnReefDriver: {0}", applicationId);
    this.hostApplicationId = applicationId;
  }

  /** StartTime event: launch another REEF job. */
  @Override
  public void onNext(final StartTime startTime) {

    LOG.log(Level.INFO, "Driver started: app {0} :: {1}", new Object[] {this.hostApplicationId, startTime});
    LOG.log(Level.FINE, "Launching Unmanaged AM: {0}", JAR_PATH);

    try (final DriverLauncher client = DriverLauncher.getLauncher(RUNTIME_CONFIG)) {

      final String innerApplicationId = client.submit(DRIVER_CONFIG, 10000);
      LOG.log(Level.INFO, "REEF-on-REEF job submitted: host app {0} new app {1}",
          new Object[] {this.hostApplicationId, innerApplicationId});

      final Configuration yarnAmConfig = UnmanagedAmYarnDriverConfiguration.CONF
          .set(UnmanagedAmYarnDriverConfiguration.JOB_IDENTIFIER, innerApplicationId)
          .set(UnmanagedAmYarnDriverConfiguration.JOB_SUBMISSION_DIRECTORY, DRIVER_ROOT_PATH)
          .build();

      try (final REEFEnvironment reef =
          REEFEnvironment.fromConfiguration(client.getUser(), yarnAmConfig, DRIVER_CONFIG)) {

        reef.run();

        final ReefServiceProtos.JobStatusProto status = reef.getLastStatus();

        LOG.log(Level.INFO, "REEF-on-REEF inner job {0} completed: state {1}",
            new Object[] {innerApplicationId, status.getState()});
      }

      LOG.log(Level.INFO,
          "REEF-on-REEF host job {0} completed: inner app {1} status {2}",
          new Object[] {this.hostApplicationId, innerApplicationId, client.getStatus()});

    } catch (final InjectionException ex) {
      LOG.log(Level.SEVERE, "REEF-on-REEF configuration error", ex);
      throw new RuntimeException("REEF-on-REEF configuration error", ex);
    }

    ThreadLogger.logThreads(LOG, Level.FINEST, "Threads running after DriverLauncher.close():");
  }
}
