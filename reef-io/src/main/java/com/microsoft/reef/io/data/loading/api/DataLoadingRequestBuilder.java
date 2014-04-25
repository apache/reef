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
package com.microsoft.reef.io.data.loading.api;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;

import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.DriverConfigurationOptions;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.io.data.loading.impl.EvaluatorRequestSerializer;
import com.microsoft.reef.io.data.loading.impl.InputFormatExternalConstructor;
import com.microsoft.reef.io.data.loading.impl.InputFormatLoadingService;
import com.microsoft.reef.io.data.loading.impl.WritableSerializer;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.wake.time.Clock;

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
  
  @NamedParameter(default_value="NULL")
  public static final class DataLoadingComputeRequest implements Name<String> {
  }
  
  @NamedParameter(default_value="false")
  public static final class LoadDataIntoMemory implements Name<Boolean> {
  }
  
  private int memoryMB = -1;
  private JobConf jobConf = null;
  private int numberOfDesiredSplits = -1;
  private EvaluatorRequest computeRequest = null;
  private boolean inMemory = false;
  private ConfigurationModule driverConfigurationModule = null;

  public DataLoadingRequestBuilder setNumberOfDesiredSplits(int numberOfDesiredSplits) {
    this.numberOfDesiredSplits = numberOfDesiredSplits;
    return this;
  }

  /**
   * Set the memory to be used for Evaluator allocated.
   *
   * @param memoryMB the amount of memory in MB
   * @return this
   */
  public DataLoadingRequestBuilder setMemoryMB(int memoryMB) {
    this.memoryMB = memoryMB;
    return this;
  }


  /**
   * Set the jobConf to use for the InputFormat configuration.
   *
   * @param jobConf
   * @return this
   */
  public DataLoadingRequestBuilder setJobConf(JobConf jobConf) {
    this.jobConf = jobConf;
    return this;
  }

  public DataLoadingRequestBuilder setComputeRequest(EvaluatorRequest computeRequest){
    this.computeRequest = computeRequest;
    return this;
  }
  
  public DataLoadingRequestBuilder loadIntoMemory(boolean inMemory){
    this.inMemory = inMemory;
    return this;
  }
  
  public DataLoadingRequestBuilder setDriverConfigurationModule(ConfigurationModule driverConfigurationModule){
    this.driverConfigurationModule = driverConfigurationModule;
    return this;
  }

  @Override
  public Configuration build() {
    jobConf.set("fs.hdfs.impl",
        org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    jobConf.set("fs.file.impl",
        org.apache.hadoop.fs.LocalFileSystem.class.getName());
    try {
      if(driverConfigurationModule==null){
        throw new IllegalStateException("Missing Driver Configuration Module is a required parameter.");
      }
      final Configuration driverConfiguration = driverConfigurationModule
          .set(DriverConfiguration.ON_DRIVER_STARTED, DataLoader.StartHandler.class)
          .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, DataLoader.EvaluatorAllocatedHandler.class)
          .set(DriverConfiguration.ON_EVALUATOR_FAILED, DataLoader.FailedEvaluatorHandler.class)
          .build();

      JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder(driverConfiguration);
      if(numberOfDesiredSplits>0){
        jcb.bindNamedParameter(NumberOfDesiredSplits.class,
            Integer.toString(numberOfDesiredSplits));
      }
      if(memoryMB>0){
        jcb.bindNamedParameter(DataLoadingEvaluatorMemoryMB.class,
            Integer.toString(memoryMB));
      }
      if(computeRequest!=null){
        jcb.bindNamedParameter(DataLoadingComputeRequest.class, 
            EvaluatorRequestSerializer.serialize(computeRequest));
      }
      jcb.bindNamedParameter(LoadDataIntoMemory.class, Boolean.toString(inMemory));
      final Class<? extends ExternalConstructor<InputFormat<?, ?>>> inputFormatExternalConstructorClass =
          (Class<? extends ExternalConstructor<InputFormat<?, ?>>>) InputFormatExternalConstructor.class;
      jcb.bindConstructor(
          InputFormat.class,
          inputFormatExternalConstructorClass);
      return jcb            
          .bindNamedParameter(InputFormatExternalConstructor.SerializedJobConf.class,
              WritableSerializer.serialize(jobConf))
          .bindImplementation(DataLoadingService.class,
              InputFormatLoadingService.class)
          .build();
    } catch (BindException e) {
      throw new RuntimeException("Unable to convert JobConf to TangConf", e);
    }
  }
}
