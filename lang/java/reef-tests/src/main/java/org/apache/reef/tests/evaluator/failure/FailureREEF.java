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
package org.apache.reef.tests.evaluator.failure;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.tests.evaluator.failure.parameters.NumEvaluatorsToFail;
import org.apache.reef.tests.evaluator.failure.parameters.NumEvaluatorsToSubmit;
import org.apache.reef.util.EnvironmentUtils;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Entry point class for REEF failure test.
 */
public final class FailureREEF {
  /**
   * The upper limit on the number of Evaluators that the local resourcemanager will hand out concurrently.
   */
  public static final int MAX_NUMBER_OF_EVALUATORS = 16;

  private static final Logger LOG = Logger.getLogger(FailureREEF.class.getName());

  private static Configuration parseCommandLine(final String[] aArgs) {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    try {
      new CommandLine(cb)
          .registerShortNameOfClass(Local.class)
          .registerShortNameOfClass(TimeOut.class)
          .processCommandLine(aArgs);
      return cb.build();
    } catch (final BindException | IOException ex) {
      final String msg = "Unable to parse command line";
      LOG.log(Level.SEVERE, msg, ex);
      throw new RuntimeException(msg, ex);
    }
  }

  /**
   * @return (immutable) TANG Configuration object.
   * @throws BindException      if configuration injector fails.
   * @throws InjectionException if the Local.class parameter is not injected.
   */
  private static Configuration getRunTimeConfiguration(final boolean isLocal) throws BindException {

    final Configuration runtimeConfiguration;

    if (isLocal) {
      LOG.log(Level.INFO, "Running Failure demo on the local runtime");
      runtimeConfiguration = LocalRuntimeConfiguration.CONF
          .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, MAX_NUMBER_OF_EVALUATORS)
          .build();
    } else {
      LOG.log(Level.INFO, "Running Failure demo on YARN");
      runtimeConfiguration = YarnClientConfiguration.CONF.build();
    }

    return runtimeConfiguration;
  }

  public static LauncherStatus runFailureReef(
      final Configuration runtimeConfig, final int timeout, final int numEvaluatorsToSubmit,
      final int numEvaluatorsToFail) throws InjectionException {

    final Configuration driverConf = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(FailureDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "FailureREEF")
        .set(DriverConfiguration.ON_DRIVER_STARTED, FailureDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, FailureDriver.EvaluatorAllocatedHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_FAILED, FailureDriver.EvaluatorFailedHandler.class)
        .set(DriverConfiguration.ON_DRIVER_STOP, FailureDriver.StopHandler.class)
        .build();

    final Configuration namedParamsConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(NumEvaluatorsToSubmit.class, Integer.toString(numEvaluatorsToSubmit))
        .bindNamedParameter(NumEvaluatorsToFail.class, Integer.toString(numEvaluatorsToFail))
        .build();

    final LauncherStatus state = DriverLauncher.getLauncher(runtimeConfig)
        .run(Configurations.merge(driverConf, namedParamsConf), timeout);

    LOG.log(Level.INFO, "REEF job completed: {0}", state);
    return state;
  }

  public static void main(final String[] args) throws InjectionException {
    final Configuration commandLineConf = parseCommandLine(args);
    final Injector injector = Tang.Factory.getTang().newInjector(commandLineConf);
    final boolean isLocal = injector.getNamedInstance(Local.class);
    final int jobTimeout = injector.getNamedInstance(TimeOut.class) * 60 * 1000;
    runFailureReef(getRunTimeConfiguration(isLocal), jobTimeout, 40, 10);
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private FailureREEF() {
  }

  /**
   * Command line parameter = true to run locally, or false to run on YARN.
   */
  @NamedParameter(doc = "Whether or not to run on the local runtime",
      short_name = "local", default_value = "true")
  public static final class Local implements Name<Boolean> {
  }

  /**
   * Number of minutes before timeout.
   */
  @NamedParameter(doc = "Number of minutes before timeout",
      short_name = "timeout", default_value = "2")
  public static final class TimeOut implements Name<Integer> {
  }
}
