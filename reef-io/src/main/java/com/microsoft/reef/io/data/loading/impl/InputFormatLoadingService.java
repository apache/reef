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
package com.microsoft.reef.io.data.loading.impl;

import java.io.IOException;
import java.util.Random;
import java.util.logging.Logger;

import javax.inject.Inject;

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.ContextConfiguration;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.io.data.loading.api.DataLoadingRequestBuilder;
import com.microsoft.reef.io.data.loading.api.DataLoadingService;
import com.microsoft.reef.io.data.loading.api.DataSet;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.BindException;

/**
 * An implementation of {@link DataLoadingService}
 * that uses the Hadoop {@link InputFormat} to find
 * partitions of data & request resources.
 * 
 * The InputFormat is injected using a Tang external constructor
 * 
 * It also tries to obtain data locality in a greedy
 * fashion using {@link EvaluatorToPartitionMapper}
 */
@DriverSide
public class InputFormatLoadingService<K,V> implements DataLoadingService {
  private static final Logger LOG = Logger.getLogger(InputFormatLoadingService.class.getName());
  private final EvaluatorToPartitionMapper<InputSplit> evaluatorToPartitionMapper;
  private final int numberOfPartitions;
  private final String serializedJobConf;
  private final String computeContextPrefix 
      = "ComputeContext-" + (new Random(3381)).nextInt(1 << 20) + "-";
  private final boolean inMemory;
  
  @Inject
  public InputFormatLoadingService(
      InputFormat<K, V> inputFormat,
      @Parameter(InputFormatExternalConstructor.SerializedJobConf.class) String serializedJobConf,
      @Parameter(DataLoadingRequestBuilder.NumberOfDesiredSplits.class) int numberOfDesiredSplits,
      @Parameter(DataLoadingRequestBuilder.LoadDataIntoMemory.class) boolean inMemory
  ) {
    this.serializedJobConf = serializedJobConf;
    this.inMemory = inMemory;
    final JobConf jobConf = WritableSerializer.deserialize(serializedJobConf);
    try {
      final InputSplit[] inputSplits = inputFormat.getSplits(jobConf, numberOfDesiredSplits);
      for (InputSplit inputSplit : inputSplits) {
        LOG.info("Split-" + inputSplit.toString());
      }
      this.numberOfPartitions = inputSplits.length;
      LOG.info("Number of Partitions: " + numberOfPartitions);
      this.evaluatorToPartitionMapper = new EvaluatorToPartitionMapper<>(inputSplits);
    } catch (IOException e) {
      throw new RuntimeException("Unable to get InputSplits using the specified InputFormat", e);
    }
  }

  @Override
  public int getNumberOfPartitions() {
    return numberOfPartitions;
  }

  @Override
  public Configuration getConfiguration(AllocatedEvaluator allocatedEvaluator) {
    try {
      final NumberedSplit<InputSplit> numberedSplit = evaluatorToPartitionMapper.getInputSplit(allocatedEvaluator.getId());
      final Configuration contextIdConfiguration = ContextConfiguration.CONF
          .set(ContextConfiguration.IDENTIFIER, "DataLoadContext-" + numberedSplit.getIndex())
          .build();
      final Tang tang = Tang.Factory.getTang();
      final JavaConfigurationBuilder jcb = tang.newConfigurationBuilder(contextIdConfiguration);
      if(inMemory){
        jcb.bindImplementation(DataSet.class, InMemoryInputFormatDataSet.class);
      }
      else{
        jcb.bindImplementation(DataSet.class, InputFormatDataSet.class);
      }
      jcb.bindNamedParameter(InputFormatExternalConstructor.SerializedJobConf.class,
          serializedJobConf);
      jcb.bindNamedParameter(InputSplitExternalConstructor.SerializedInputSplit.class, WritableSerializer.serialize(numberedSplit.getEntry()));
      jcb.bindConstructor(
          InputSplit.class,
          (Class<? extends ExternalConstructor<InputSplit>>) InputSplitExternalConstructor.class
      );
      return jcb.build();
    } catch (BindException e) {
      throw new RuntimeException("Unable to create Configuration", e);
    }
  }

  @Override
  public String getComputeContextIdPrefix() {
    return computeContextPrefix;
  }

  @Override
  public boolean isDataLoadedContext(ActiveContext context) {
    return !context.getId().startsWith(computeContextPrefix);
  }
}
