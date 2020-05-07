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
import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.runtime.common.REEFEnvironment;
import org.apache.reef.runtime.common.driver.parameters.ClientRemoteIdentifier;
import org.apache.reef.runtime.common.launch.REEFMessageCodec;
import org.apache.reef.runtime.local.driver.LocalDriverConfiguration;
import org.apache.reef.runtime.local.driver.RuntimeIdentifier;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.wake.remote.RemoteConfiguration;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Client for Hello REEF example running driver and client in the same process.
 */
public final class HelloREEFEnvironment {

  private static final Logger LOG = Logger.getLogger(HelloREEFEnvironment.class.getName());

  private static final Configuration DRIVER_CONFIG = DriverConfiguration.CONF
      .set(DriverConfiguration.DRIVER_IDENTIFIER, "HelloREEF_Env")
      .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(HelloDriver.class))
      .set(DriverConfiguration.ON_DRIVER_STARTED, HelloDriver.StartHandler.class)
      .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, HelloDriver.EvaluatorAllocatedHandler.class)
      .build();

  private static final Configuration LOCAL_DRIVER_MODULE = LocalDriverConfiguration.CONF
      .set(LocalDriverConfiguration.RUNTIME_NAMES, RuntimeIdentifier.RUNTIME_NAME)
      .set(LocalDriverConfiguration.CLIENT_REMOTE_IDENTIFIER, ClientRemoteIdentifier.NONE)
      .set(LocalDriverConfiguration.JOB_IDENTIFIER, "LOCAL_ENV_DRIVER_TEST")
      .set(LocalDriverConfiguration.ROOT_FOLDER, "./REEF_LOCAL_RUNTIME")
      .set(LocalDriverConfiguration.MAX_NUMBER_OF_EVALUATORS, 2)
      .set(LocalDriverConfiguration.JVM_HEAP_SLACK, 0.0)
      .build();

  private static final Configuration ENVIRONMENT_CONFIG =
      Tang.Factory.getTang().newConfigurationBuilder()
          .bindNamedParameter(RemoteConfiguration.ManagerName.class, "REEF_ENVIRONMENT")
          .bindNamedParameter(RemoteConfiguration.MessageCodec.class, REEFMessageCodec.class)
          .build();

  /**
   * Start Hello REEF job with Driver and Client sharing the same process.
   *
   * @param args command line parameters - not used.
   * @throws InjectionException configuration error.
   */
  public static void main(final String[] args) throws InjectionException {

    try (REEFEnvironment reef = REEFEnvironment.fromConfiguration(
        LOCAL_DRIVER_MODULE, DRIVER_CONFIG, ENVIRONMENT_CONFIG)) {
      reef.run();
      final ReefServiceProtos.JobStatusProto status = reef.getLastStatus();
      LOG.log(Level.INFO, "REEF job completed: {0}", status);
    }
  }

  /**
   * Empty private constructor to prohibit instantiation of all-static class.
   */
  private HelloREEFEnvironment() {
  }
}
