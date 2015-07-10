/**
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
package org.apache.reef.examples.shuffle;

import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.io.data.loading.api.DataLoadingRequestBuilder;
import org.apache.reef.io.network.naming.NameResolver;
import org.apache.reef.io.network.shuffle.driver.ShuffleDriverConfiguration;
import org.apache.reef.io.network.shuffle.utils.NameResolverWrapper;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.util.EnvironmentUtils;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 */
public class WordCountREEF {
  private static final Logger LOG = Logger.getLogger(WordCountREEF.class.getName());

  /**
   * The upper limit on the number of Evaluators that the local resourcemanager will hand out concurrently.
   */
  private static final int MAX_NUMBER_OF_EVALUATORS = 16;

  /**
   * Number of milliseconds to wait for the job to complete.
   */
  private static final int JOB_TIMEOUT = 200000; // 100 sec.


  public static Configuration getRuntimeConfiguration(final boolean isLocal) {
    if (isLocal) {
      return LocalRuntimeConfiguration.CONF
          .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, MAX_NUMBER_OF_EVALUATORS)
          .build();
    } else {
      return YarnClientConfiguration.CONF.build();
    }
  }

  public static Configuration getDriverConfiguration(final int mapperNum, final int reducerNum, final String inputDir) {

    final Configuration nsClientConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(NameResolver.class, NameResolverWrapper.class)
        .build();

    final EvaluatorRequest computeRequest = EvaluatorRequest.newBuilder()
        .setNumber(reducerNum + 1)
        .setMemory(256)
        .setNumberOfCores(1)
        .build();

    final Configuration dataLoadConfiguration = new DataLoadingRequestBuilder()
        .setMemoryMB(256)
        .setInputFormatClass(TextInputFormat.class)
        .setInputPath(inputDir)
        .setNumberOfDesiredSplits(mapperNum)
        .setComputeRequest(computeRequest)
        .setDriverConfigurationModule(DriverConfiguration.CONF
            .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(WordCountDriver.class))
            .set(DriverConfiguration.ON_CONTEXT_ACTIVE, WordCountDriver.ContextActiveHandler.class)
            .set(DriverConfiguration.DRIVER_IDENTIFIER, "WordCountREEF"))
        .build();

    final JavaConfigurationBuilder driverConfBuilder = Tang.Factory.getTang().newConfigurationBuilder(
        ShuffleDriverConfiguration.CONF.build(),
        nsClientConf,
        dataLoadConfiguration);

    return driverConfBuilder
        .bindNamedParameter(MapperNum.class, String.valueOf(mapperNum))
        .bindNamedParameter(ReducerNum.class, String.valueOf(reducerNum))
        .build();
  }

  public static void main(final String[] args) throws Exception {

    final Tang tang = Tang.Factory.getTang();

    final JavaConfigurationBuilder commandParamConf = tang.newConfigurationBuilder();

    new CommandLine(commandParamConf)
        .registerShortNameOfClass(MapperNum.class)
        .registerShortNameOfClass(ReducerNum.class)
        .registerShortNameOfClass(InputDir.class)
        .registerShortNameOfClass(Local.class)
        .processCommandLine(args);

    final Injector commandParamInjector = tang.newInjector(commandParamConf.build());

    final boolean isLocal = commandParamInjector.getNamedInstance(Local.class);
    final int mapperNum = commandParamInjector.getNamedInstance(MapperNum.class);
    final int reducerNum = commandParamInjector.getNamedInstance(ReducerNum.class);
    final String inputDir = commandParamInjector.getNamedInstance(InputDir.class);

    if (mapperNum + reducerNum > MAX_NUMBER_OF_EVALUATORS) {
      throw new Exception(
          "The sum of mapper and reducer number should less than " + MAX_NUMBER_OF_EVALUATORS
          + " [ mapper num : " + mapperNum + " , reducer num : " + reducerNum + " is not allowed ]"
      );
    }


    final LauncherStatus status = DriverLauncher.getLauncher(getRuntimeConfiguration(isLocal))
        .run(getDriverConfiguration(mapperNum, reducerNum, inputDir), JOB_TIMEOUT);

    LOG.log(Level.INFO, "REEF job completed: {0}", status);
  }

  @NamedParameter(short_name = "mapper_num", default_value = "5")
  public static class MapperNum implements Name<Integer> {
  }

  @NamedParameter(short_name = "reducer_num", default_value = "3")
  public static class ReducerNum implements Name<Integer> {
  }

  @NamedParameter(short_name = "input")
  public static class InputDir implements Name<String> {
  }

  @NamedParameter(short_name = "local", default_value = "true")
  public static class Local implements Name<Boolean> {
  }
}