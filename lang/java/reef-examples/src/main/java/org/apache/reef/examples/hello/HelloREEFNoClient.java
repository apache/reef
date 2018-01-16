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
import org.apache.reef.client.REEF;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.EnvironmentUtils;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A main() for running hello REEF without a persistent client connection.
 */
public final class HelloREEFNoClient {

  private static final Logger LOG = Logger.getLogger(HelloREEFNoClient.class.getName());

  /**
   * The upper limit on the number of Evaluators that the local resourcemanager will hand out concurrently.
   */
  private static final int MAX_NUMBER_OF_EVALUATORS = 2;

  /**
   * @return the configuration of the runtime
   */
  private static Configuration getRuntimeConfiguration() {
    return LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, MAX_NUMBER_OF_EVALUATORS)
        .build();
  }

  /**
   * @return the configuration of the HelloREEF driver.
   */
  private static Configuration getDriverConfiguration() {
    return DriverConfiguration.CONF
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "HelloREEF")
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(HelloDriver.class))
        .set(DriverConfiguration.ON_DRIVER_STARTED, HelloDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, HelloDriver.EvaluatorAllocatedHandler.class)
        .build();
  }

  /**
   * Used in the HDInsight example.
   * @param runtimeConf the configuration of the runtime
   * @throws InjectionException
   */
  public static void runHelloReefWithoutClient(final Configuration runtimeConf) throws InjectionException {
    final REEF reef = Tang.Factory.getTang().newInjector(runtimeConf).getInstance(REEF.class);
    final Configuration driverConf = getDriverConfiguration();
    reef.submit(driverConf);
  }

  public static void main(final String[] args) throws BindException, InjectionException {
    final Configuration runtimeConf = getRuntimeConfiguration();
    final Configuration driverConf = getDriverConfiguration();

    final REEF reef = Tang.Factory.getTang().newInjector(runtimeConf).getInstance(REEF.class);
    reef.submit(driverConf);
    LOG.log(Level.INFO, "Job Submitted");
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private HelloREEFNoClient() {
  }
}
