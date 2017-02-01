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
package org.apache.reef.tests.examples;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.examples.hello.HelloDriver;
import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.runtime.common.REEFEnvironment;
import org.apache.reef.runtime.yarn.client.unmanaged.UnmanagedAmYarnClientConfiguration;
import org.apache.reef.runtime.yarn.client.unmanaged.UnmanagedAmYarnDriverConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.util.ThreadLogger;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Client for running HelloREEF on YARN in unmanaged AM mode.
 */
public final class TestHelloREEFYarnUnmanagedAm {

  private static final Logger LOG = Logger.getLogger(TestHelloREEFYarnUnmanagedAm.class.getName());

  private static final String DRIVER_ROOT_PATH = ".";
  private static final String JAR_PATH = EnvironmentUtils.getClassLocation(TestHelloREEFYarnUnmanagedAm.class);

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
   * @throws InjectionException configuration error.
   */
  @Test
  public void testHelloREEF() throws InjectionException {

    Assume.assumeTrue(
        "This test requires a YARN Resource Manager to connect to",
        Boolean.parseBoolean(System.getenv("REEF_TEST_YARN")));

    LOG.log(Level.FINE, "Launching Unnmanaged AM: {0}", JAR_PATH);

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
        final ReefServiceProtos.State jobState = status.getState();
        LOG.log(Level.INFO, "REEF job {0} completed: state {1}", new Object[] {appId, jobState});
        Assert.assertEquals("Job state after execution: " + jobState, jobState, ReefServiceProtos.State.DONE);
      }

      final LauncherStatus clientStatus = client.getStatus();
      LOG.log(Level.INFO, "REEF job {0} completed: client status {1}", new Object[] {appId, clientStatus});
      Assert.assertTrue("Job state after execution: " + clientStatus, clientStatus.isSuccess());
    }

    ThreadLogger.logThreads(LOG, Level.FINEST, "Threads running after DriverLauncher.close():");

    LOG.log(Level.INFO, "Clean exit!");
  }
}
