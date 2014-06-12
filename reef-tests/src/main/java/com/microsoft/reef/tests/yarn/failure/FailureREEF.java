/*
 * Copyright 2013 Microsoft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.tests.yarn.failure;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.DriverLauncher;
import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.reef.runtime.yarn.client.YarnClientConfiguration;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.CommandLine;

/**
 *
 */
public class FailureREEF {

  private static final Logger LOG = Logger.getLogger(FailureREEF.class
      .getName());

  private static boolean local;
  /**
   * Number of milliseconds to wait for the job to complete.
   */
  private static int JOB_TIMEOUT;

  /**
   * Command line parameter = true to run locally, or false to run on YARN.
   */
  @NamedParameter(doc = "Whether or not to run on the local runtime", short_name = "local", default_value = "true")
  public static final class Local implements Name<Boolean> {
  }

  @NamedParameter(doc="Number of minutes before timeout", short_name="timeout",default_value="2")
  public static final class TimeOut implements Name<Integer>{
  }

  private static Configuration parseCommandLine(final String[] aArgs) {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    try {
      final CommandLine cl = new CommandLine(cb);
      cl.registerShortNameOfClass(Local.class);
      cl.registerShortNameOfClass(TimeOut.class);
      cl.processCommandLine(aArgs);
    } catch (final BindException | IOException ex) {
      final String msg = "Unable to parse command line";
      LOG.log(Level.SEVERE, msg, ex);
      throw new RuntimeException(msg, ex);
    }
    return cb.build();
  }

  /**
   * copy the parameters from the command line required for the Client configuration
   */
  private static void storeCommandLineArgs(final Configuration commandLineConf)
      throws InjectionException, BindException {
    final Injector injector = Tang.Factory.getTang().newInjector(commandLineConf);
    local = injector.getNamedInstance(Local.class);
    JOB_TIMEOUT = injector.getNamedInstance(TimeOut.class) * 60 * 1000;
  }

  /**
   * @param commandLineConf Command line arguments, as passed into main().
   * @return (immutable) TANG Configuration object.
   * @throws BindException      if configuration injector fails.
   * @throws InjectionException if the Local.class parameter is not injected.
   */
  private static Configuration getRunTimeConfiguration() throws BindException {
    final Configuration runtimeConfiguration;
    if (local) {
      LOG.log(Level.INFO, "Running Failure demo on the local runtime");
      runtimeConfiguration = LocalRuntimeConfiguration.CONF
          .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, 40)
          .build();
    } else {
      LOG.log(Level.INFO, "Running Failure demo on YARN");
      runtimeConfiguration = YarnClientConfiguration.CONF.build();
    }
    return runtimeConfiguration;
  }

  public static LauncherStatus runFailureReef(final Configuration runtimeConfig, final int timeout) throws InjectionException {
    final Configuration driverConf =
        EnvironmentUtils.addClasspath(DriverConfiguration.CONF, DriverConfiguration.GLOBAL_LIBRARIES)
            .set(DriverConfiguration.DRIVER_IDENTIFIER, "FailureREEF")
            .set(DriverConfiguration.ON_DRIVER_STARTED, FailureDriver.StartHandler.class)
            .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, FailureDriver.EvaluatorAllocatedHandler.class)
            .set(DriverConfiguration.ON_EVALUATOR_FAILED, FailureDriver.FailedEvaluatorHandler.class)
            .build();

    final LauncherStatus state = DriverLauncher.getLauncher(runtimeConfig).run(driverConf, timeout);
    return state;
  }

  public static void main(final String[] args) throws InjectionException {
    final Configuration commandLineConf = parseCommandLine(args);
    storeCommandLineArgs(commandLineConf);

    final LauncherStatus state = runFailureReef(getRunTimeConfiguration(), JOB_TIMEOUT);
    LOG.log(Level.INFO, "REEF job completed: {0}", state);
  }


}
