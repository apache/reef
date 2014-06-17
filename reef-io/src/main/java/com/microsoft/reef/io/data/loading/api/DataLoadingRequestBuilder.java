/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.io.data.loading.api;

import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.io.data.loading.impl.EvaluatorRequestSerializer;
import com.microsoft.reef.io.data.loading.impl.InputFormatExternalConstructor;
import com.microsoft.reef.io.data.loading.impl.InputFormatLoadingService;
import com.microsoft.reef.io.data.loading.impl.WritableSerializer;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.formats.ConfigurationModule;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;

/**
 * Builder to create a request to the DataLoadingService.
 */
public final class DataLoadingRequestBuilder implements com.microsoft.reef.util.Builder<Configuration> {

  @NamedParameter(short_name = "num_splits", default_value = "0")
  public static final class NumberOfDesiredSplits implements Name<Integer> {
  }

  @NamedParameter(short_name = "dataLoadingEvaluatorMemoryMB", default_value = "4096")
  public static final class DataLoadingEvaluatorMemoryMB implements Name<Integer> {
  }

  @NamedParameter(default_value = "NULL")
  public static final class DataLoadingComputeRequest implements Name<String> {
  }

  @NamedParameter(default_value = "false")
  public static final class LoadDataIntoMemory implements Name<Boolean> {
  }

  private int memoryMB = -1;
  private JobConf jobConf = null;
  private int numberOfDesiredSplits = -1;
  private EvaluatorRequest computeRequest = null;
  private boolean inMemory = false;
  private ConfigurationModule driverConfigurationModule = null;

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
   * Set the jobConf to use for the InputFormat configuration.
   */
  public DataLoadingRequestBuilder setJobConf(final JobConf jobConf) {
    this.jobConf = jobConf;
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

  public DataLoadingRequestBuilder setDriverConfigurationModule(
      final ConfigurationModule driverConfigurationModule) {
    this.driverConfigurationModule = driverConfigurationModule;
    return this;
  }

  @Override
  public Configuration build() {

    this.jobConf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    this.jobConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

    try {

      if (this.driverConfigurationModule == null) {
        throw new IllegalStateException("Driver Configuration Module is a required parameter.");
      }

      final Configuration driverConfiguration = this.driverConfigurationModule
          .set(DriverConfiguration.ON_DRIVER_STARTED, DataLoader.StartHandler.class)
          .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, DataLoader.EvaluatorAllocatedHandler.class)
          .set(DriverConfiguration.ON_EVALUATOR_FAILED, DataLoader.FailedEvaluatorHandler.class)
          .build();

      final JavaConfigurationBuilder jcb =
          Tang.Factory.getTang().newConfigurationBuilder(driverConfiguration);

      if (this.numberOfDesiredSplits > 0) {
        jcb.bindNamedParameter(NumberOfDesiredSplits.class, "" + this.numberOfDesiredSplits);
      }

      if (this.memoryMB > 0) {
        jcb.bindNamedParameter(DataLoadingEvaluatorMemoryMB.class, "" + this.memoryMB);
      }

      if (this.computeRequest != null) {
        jcb.bindNamedParameter(DataLoadingComputeRequest.class,
            EvaluatorRequestSerializer.serialize(this.computeRequest));
      }

      return jcb
          .bindNamedParameter(LoadDataIntoMemory.class, "" + this.inMemory)
          .bindConstructor(InputFormat.class, InputFormatExternalConstructor.class)
          .bindNamedParameter(
              InputFormatExternalConstructor.SerializedJobConf.class,
              WritableSerializer.serialize(this.jobConf))
          .bindImplementation(DataLoadingService.class, InputFormatLoadingService.class)
          .build();

    } catch (final BindException e) {
      throw new RuntimeException("Unable to convert JobConf to TangConf", e);
    }
  }
}
