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
package org.apache.reef.tests.driver;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.runtime.common.REEFEnvironment;
import org.apache.reef.runtime.common.driver.parameters.ClientRemoteIdentifier;
import org.apache.reef.runtime.local.driver.LocalDriverConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.EnvironmentUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * This tests whether the noop driver launched in-process gets shutdown properly.
 */
public final class REEFEnvironmentDriverTest {

  private static final Configuration DRIVER_CONFIG = DriverConfiguration.CONF
      .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(DriverTestStartHandler.class))
      .set(DriverConfiguration.DRIVER_IDENTIFIER, "TEST_REEFEnvironmentDriverTest")
      .set(DriverConfiguration.ON_DRIVER_STARTED, DriverTestStartHandler.class)
      .build();

  private static final Configuration LOCAL_DRIVER_MODULE = LocalDriverConfiguration.CONF
      .set(LocalDriverConfiguration.MAX_NUMBER_OF_EVALUATORS, 1)
      .set(LocalDriverConfiguration.ROOT_FOLDER, ".")
      .set(LocalDriverConfiguration.JVM_HEAP_SLACK, 0.0)
      .set(LocalDriverConfiguration.CLIENT_REMOTE_IDENTIFIER, ClientRemoteIdentifier.NONE)
      .set(LocalDriverConfiguration.JOB_IDENTIFIER, "LOCAL_ENV_DRIVER_TEST")
      .set(LocalDriverConfiguration.RUNTIME_NAMES, org.apache.reef.runtime.local.driver.RuntimeIdentifier.RUNTIME_NAME)
      .build();

  @Test
  public void testREEFEnvironmentDriver() throws BindException, InjectionException {

    try (final REEFEnvironment reef = REEFEnvironment.fromConfiguration(LOCAL_DRIVER_MODULE, DRIVER_CONFIG)) {

      reef.run();
      final ReefServiceProtos.JobStatusProto status = reef.getLastStatus();

      Assert.assertNotNull("REEF job must report its status", status);
      Assert.assertTrue("REEF job status must contain a state", status.hasState());
      Assert.assertEquals("Unexpected final job status", ReefServiceProtos.State.DONE, status.getState());

    } catch (final Throwable ex) {
      Assert.fail("Local driver execution failed: " + ex);
    }
  }
}
