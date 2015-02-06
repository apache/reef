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
package org.apache.reef.io.data.loading.api;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.io.data.loading.impl.EvaluatorRequestSerializer;
import org.apache.reef.io.data.loading.impl.InputFormatExternalConstructor;
import org.apache.reef.io.data.loading.impl.InputFormatLoadingService;
import org.apache.reef.io.data.loading.impl.JobConfExternalConstructor;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.formats.ConfigurationModule;

/**
 * Builder to create a request to the DataLoadingService.
 */
public final class DataLoadingRequestBuilder
    implements org.apache.reef.util.Builder<Configuration> {

  private int memoryMB = -1;
  private int numberOfCores = -1;
  private int numberOfDesiredSplits = -1;
  private EvaluatorRequest computeRequest = null;
  private boolean inMemory = false;
  private boolean renewFailedEvaluators = true;
  private ConfigurationModule driverConfigurationModule = null;
  private String inputFormatClass;
  private String inputPath;

  public DataLoadingRequestBuilder setNumberOfDesiredSplits(final int numberOfDesiredSplits) {
    this.numberOfDesiredSplits = numberOfDesiredSplits;
    return this;
  }

  /**
   * Set the memory to be used for Evaluator allocated.
   *
   * @param memoryMB the amount of memory in MB
   * @return this
   */
  public DataLoadingRequestBuilder setMemoryMB(final int memoryMB) {
    this.memoryMB = memoryMB;
    return this;
  }

  /**
   * Set the core number to be used for Evaluator allocated.
   *
   * @param numberOfCores the number of cores
   * @return this
   */
  public DataLoadingRequestBuilder setNumberOfCores(final int numberOfCores) {
    this.numberOfCores = numberOfCores;
    return this;
  }

  public DataLoadingRequestBuilder setComputeRequest(final EvaluatorRequest computeRequest) {
    this.computeRequest = computeRequest;
    return this;
  }

  public DataLoadingRequestBuilder loadIntoMemory(final boolean inMemory) {
    this.inMemory = inMemory;
    return this;
  }

  public DataLoadingRequestBuilder renewFailedEvaluators(final boolean renewFailedEvaluators) {
    this.renewFailedEvaluators = renewFailedEvaluators;
    return this;
  }

  public DataLoadingRequestBuilder setDriverConfigurationModule(
      final ConfigurationModule driverConfigurationModule) {
    this.driverConfigurationModule = driverConfigurationModule;
    return this;
  }

  public DataLoadingRequestBuilder setInputFormatClass(
      final Class<? extends InputFormat> inputFormatClass) {
    this.inputFormatClass = inputFormatClass.getName();
    return this;
  }

  public DataLoadingRequestBuilder setInputPath(final String inputPath) {
    this.inputPath = inputPath;
    return this;
  }

  @Override
  public Configuration build() throws BindException {
    if (this.driverConfigurationModule == null) {
      throw new BindException("Driver Configuration Module is a required parameter.");
    }

    if (this.inputPath == null) {
      throw new BindException("InputPath is a required parameter.");
    }

    if (this.inputFormatClass == null) {
      this.inputFormatClass = TextInputFormat.class.getName();
    }

    final Configuration driverConfiguration;
    if (renewFailedEvaluators) {
      driverConfiguration = this.driverConfigurationModule
          .set(DriverConfiguration.ON_DRIVER_STARTED, DataLoader.StartHandler.class)
          .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, DataLoader.EvaluatorAllocatedHandler.class)
          .set(DriverConfiguration.ON_EVALUATOR_FAILED, DataLoader.EvaluatorFailedHandler.class)
          .build();
    } else {
      driverConfiguration = this.driverConfigurationModule
          .set(DriverConfiguration.ON_DRIVER_STARTED, DataLoader.StartHandler.class)
          .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, DataLoader.EvaluatorAllocatedHandler.class)
          .build();
    }

    final JavaConfigurationBuilder jcb =
        Tang.Factory.getTang().newConfigurationBuilder(driverConfiguration);

    if (this.numberOfDesiredSplits > 0) {
      jcb.bindNamedParameter(NumberOfDesiredSplits.class, "" + this.numberOfDesiredSplits);
    }

    if (this.memoryMB > 0) {
      jcb.bindNamedParameter(DataLoadingEvaluatorMemoryMB.class, "" + this.memoryMB);
    }

    if (this.numberOfCores > 0) {
      jcb.bindNamedParameter(DataLoadingEvaluatorNumberOfCores.class, "" + this.numberOfCores);
    }

    if (this.computeRequest != null) {
      jcb.bindNamedParameter(DataLoadingComputeRequest.class,
          EvaluatorRequestSerializer.serialize(this.computeRequest));
    }

    return jcb
        .bindNamedParameter(LoadDataIntoMemory.class, Boolean.toString(this.inMemory))
        .bindConstructor(InputFormat.class, InputFormatExternalConstructor.class)
        .bindConstructor(JobConf.class, JobConfExternalConstructor.class)
        .bindNamedParameter(JobConfExternalConstructor.InputFormatClass.class, inputFormatClass)
        .bindNamedParameter(JobConfExternalConstructor.InputPath.class, inputPath)
        .bindImplementation(DataLoadingService.class, InputFormatLoadingService.class)
        .build();
  }

  @NamedParameter(short_name = "num_splits", default_value = "0")
  public static final class NumberOfDesiredSplits implements Name<Integer> {
  }

  @NamedParameter(short_name = "dataLoadingEvaluatorMemoryMB", default_value = "4096")
  public static final class DataLoadingEvaluatorMemoryMB implements Name<Integer> {
  }

  @NamedParameter(short_name = "dataLoadingEvaluatorCore", default_value = "1")
  public static final class DataLoadingEvaluatorNumberOfCores implements Name<Integer> {
  }

  @NamedParameter(default_value = "NULL")
  public static final class DataLoadingComputeRequest implements Name<String> {
  }

  @NamedParameter(default_value = "false")
  public static final class LoadDataIntoMemory implements Name<Boolean> {
  }
}
