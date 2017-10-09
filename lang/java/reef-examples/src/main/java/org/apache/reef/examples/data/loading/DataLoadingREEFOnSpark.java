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
package org.apache.reef.examples.data.loading;

/**
 * Client for the data loading spark demo app.
 */
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.runtime.spark.job.SparkDataLoadingRequestBuilder;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Client for the data loading demo app running on spark executors.
 */
@ClientSide
public final class DataLoadingREEFOnSpark {

  private static final Logger LOG = Logger.getLogger(DataLoadingREEFOnSpark.class.getName());

  /**
   * The upper limit on the number of Evaluators that the local resourcemanager will hand out concurrently.
   */
  private static final int MAX_NUMBER_OF_EVALUATORS = 16;

  private static final int NUM_SPLITS = 6;
  private static final int NUM_COMPUTE_EVALUATORS = 2;


  private String inputPath;


  public static void main(final String[] args)
      throws InjectionException, BindException, IOException {

    final Tang tang = Tang.Factory.getTang();


    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();

    new CommandLine(cb)
        .registerShortNameOfClass(DataLoadingREEFOnSpark.Local.class)
        .registerShortNameOfClass(DataLoadingREEFOnSpark.TimeOut.class)
        .registerShortNameOfClass(DataLoadingREEFOnSpark.InputDir.class)
        .processCommandLine(args);

    String inputPath="wasb://somefile";

    final Injector injector = tang.newInjector(cb.build());

    final boolean isLocal = injector.getNamedInstance(DataLoadingREEFOnSpark.Local.class);
    final int jobTimeout = injector.getNamedInstance(DataLoadingREEFOnSpark.TimeOut.class) * 60 * 1000;


    final String inputDir = injector.getNamedInstance(DataLoadingREEFOnSpark.InputDir.class);

    final Configuration runtimeConfiguration;
    if (isLocal) {
      LOG.log(Level.INFO, "Running Data Loading reef on spark demo on the local runtime");
      runtimeConfiguration = LocalRuntimeConfiguration.CONF
          .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, MAX_NUMBER_OF_EVALUATORS)
          .build();
    } else {
      LOG.log(Level.INFO, "Running Data Loading reef on spark demo on YARN");
      runtimeConfiguration = YarnClientConfiguration.CONF.build();
    }

    final EvaluatorRequest computeRequest = EvaluatorRequest.newBuilder()
        .setNumber(NUM_COMPUTE_EVALUATORS)
        .setMemory(512)
        .setNumberOfCores(1)
        .build();

    final EvaluatorRequest dataRequest = EvaluatorRequest.newBuilder()
        .setMemory(512)
        .setNumberOfCores(1)
        .build();

    final Configuration dataLoadConfiguration = new SparkDataLoadingRequestBuilder()
        .setInputFormatClass(TextInputFormat.class)
        .setInputPath(inputPath)
        .setNumberOfDesiredSplits(NUM_SPLITS)
        .addComputeRequest(computeRequest)
        .addDataRequest(dataRequest)
        .setDriverConfigurationModule(DriverConfiguration.CONF
            .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(LineCounter.class))
            .set(DriverConfiguration.ON_CONTEXT_ACTIVE, LineCounter.ContextActiveHandler.class)
            .set(DriverConfiguration.ON_TASK_COMPLETED, LineCounter.TaskCompletedHandler.class)
            .set(DriverConfiguration.DRIVER_IDENTIFIER, "DataLoadingREEFOnSpark"))
        .build();

    //this is the key, we call into the spark runtime to run this.
    new SparkRunner().run(runtimeConfiguration,dataLoadConfiguration,inputPath,NUM_SPLITS);

    LOG.log(Level.INFO, "REEF job completed: {0}", state);
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

  /**
   * Input path.
   */
  @NamedParameter(short_name = "input")
  public static final class InputDir implements Name<String> {
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private DataLoadingREEFOnSpark() {
  }
}

