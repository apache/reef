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
package com.microsoft.reef.examples.data.loading;

import com.microsoft.reef.annotations.audience.ClientSide;
import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.DriverLauncher;
import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.io.data.loading.api.DataLoader;
import com.microsoft.reef.io.data.loading.api.DataLoadingDriverConfiguration;
import com.microsoft.reef.io.data.loading.api.DataLoadingRequestBuilder;
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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Client for the data loading demo app
 */
@ClientSide
public class DataLoadingREEF {
  /**
   * Standard Java logger object.
   */
  private static final Logger LOG = Logger.getLogger(DataLoadingREEF.class.getName());

  private static final String NUM_LOCAL_THREADS = "20";

  /**
   * Number of milliseconds to wait for the job to complete.
   */
  private static final int JOB_TIMEOUT = 10 * 60 * 1000;

  /**
   * Command line parameter = true to run locally, or false to run on YARN.
   */
  @NamedParameter(doc = "Whether or not to run on the local runtime", short_name = "local", default_value = "true")
  public static final class Local implements Name<Boolean> {
  }

  @NamedParameter(short_name = "input")
  public static final class InputDir implements Name<String> {
  }

  private static boolean local;
  private static String input;

  private static Configuration parseCommandLine(final String[] aArgs) {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    try {
      final CommandLine cl = new CommandLine(cb);
      cl.registerShortNameOfClass(Local.class);
      cl.registerShortNameOfClass(DataLoadingREEF.InputDir.class);
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
    input = injector.getNamedInstance(DataLoadingREEF.InputDir.class);
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
      LOG.log(Level.INFO, "Running Data Loading demo on the local runtime");
      runtimeConfiguration = LocalRuntimeConfiguration.CONF
          .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, NUM_LOCAL_THREADS)
          .build();
    } else {
      LOG.log(Level.INFO, "Running Data Loading demo on YARN");
      runtimeConfiguration = YarnClientConfiguration.CONF.build();
    }
    return runtimeConfiguration;
  }

  public static LauncherStatus runDataLoadingReef(
      final Configuration runtimeConfiguration
  ) throws BindException, InjectionException {
    final JobConf jobConf = new JobConf();
    jobConf.setInputFormat(TextInputFormat.class);
    TextInputFormat.addInputPath(jobConf, new Path(input));
    EvaluatorRequest computeRequest = EvaluatorRequest.newBuilder()
        .setNumber(2)
        .setMemory(2048)
        .build();
    final Configuration dataLoadConfiguration = new DataLoadingRequestBuilder()
        .setMemoryMB(4096)
        .setJobConf(jobConf)
        .setNumberOfDesiredSplits(12)
        .setComputeRequest(computeRequest)
        .setDriverConfigurationModule(EnvironmentUtils
            .addClasspath(DriverConfiguration.CONF, DriverConfiguration.GLOBAL_LIBRARIES)
            .set(DriverConfiguration.ON_CONTEXT_ACTIVE, LineCounter.ContextActiveHandler.class)
            .set(DriverConfiguration.ON_TASK_COMPLETED, LineCounter.TaskCompletedHandler.class)
            .set(DriverConfiguration.DRIVER_IDENTIFIER, "DataLoadingREEF"))
        .build();

    final Configuration mergedDriverConfiguration = Tang.Factory.getTang()
        .newConfigurationBuilder(dataLoadConfiguration)//, driverConfiguration)
        .build();

    return DriverLauncher.getLauncher(runtimeConfiguration).run(mergedDriverConfiguration, JOB_TIMEOUT);
  }


  /**
   * @param args
   * @throws BindException
   * @throws InjectionException
   */
  public static void main(String[] args) throws InjectionException, BindException {
    final Configuration commandLineConf = parseCommandLine(args);
    storeCommandLineArgs(commandLineConf);
    final Configuration runtimeConfiguration = getRunTimeConfiguration();
    final LauncherStatus state = runDataLoadingReef(runtimeConfiguration);
    LOG.log(Level.INFO, "REEF job completed: {0}", state);
  }

}
