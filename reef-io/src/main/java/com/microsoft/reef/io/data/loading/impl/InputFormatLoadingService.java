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
package com.microsoft.reef.io.data.loading.impl;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.ContextConfiguration;
import com.microsoft.reef.driver.context.ServiceConfiguration;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.io.data.loading.api.DataLoadingRequestBuilder;
import com.microsoft.reef.io.data.loading.api.DataLoadingService;
import com.microsoft.reef.io.data.loading.api.DataSet;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.BindException;

/**
 * An implementation of {@link DataLoadingService}
 * that uses the Hadoop {@link InputFormat} to find
 * partitions of data & request resources.
 * <p/>
 * The InputFormat is injected using a Tang external constructor
 * <p/>
 * It also tries to obtain data locality in a greedy
 * fashion using {@link EvaluatorToPartitionMapper}
 */
@DriverSide
public class InputFormatLoadingService<K, V> implements DataLoadingService {

  private static final Logger LOG = Logger.getLogger(InputFormatLoadingService.class.getName());

  private static final String DATA_LOAD_CONTEXT_PREFIX = "DataLoadContext-";

  private static final String COMPUTE_CONTEXT_PREFIX =
      "ComputeContext-" + new Random(3381).nextInt(1 << 20) + "-";

  private final EvaluatorToPartitionMapper<InputSplit> evaluatorToPartitionMapper;
  private final int numberOfPartitions;

  private final boolean inMemory;

  private final String inputFormatClass;

  private final String inputPath;

  @Inject
  public InputFormatLoadingService(
      final InputFormat<K, V> inputFormat,
      final JobConf jobConf,
      final @Parameter(DataLoadingRequestBuilder.NumberOfDesiredSplits.class) int numberOfDesiredSplits,
      final @Parameter(DataLoadingRequestBuilder.LoadDataIntoMemory.class) boolean inMemory,
      final @Parameter(JobConfExternalConstructor.InputFormatClass.class) String inputFormatClass,
      final @Parameter(JobConfExternalConstructor.InputPath.class) String inputPath) {

    this.inMemory = inMemory;
    this.inputFormatClass = inputFormatClass;
    this.inputPath = inputPath;


    try {

      final InputSplit[] inputSplits = inputFormat.getSplits(jobConf, numberOfDesiredSplits);
      if (LOG.isLoggable(Level.FINEST)) {
        LOG.log(Level.FINEST, "Splits: {0}", Arrays.toString(inputSplits));
      }

      this.numberOfPartitions = inputSplits.length;
      LOG.log(Level.FINE, "Number of partitions: {0}", this.numberOfPartitions);

      this.evaluatorToPartitionMapper = new EvaluatorToPartitionMapper<>(inputSplits);

    } catch (final IOException e) {
      throw new RuntimeException("Unable to get InputSplits using the specified InputFormat", e);
    }
  }

  @Override
  public int getNumberOfPartitions() {
    return this.numberOfPartitions;
  }

  @Override
  public Configuration getContextConfiguration(final AllocatedEvaluator allocatedEvaluator) {

    final NumberedSplit<InputSplit> numberedSplit =
        this.evaluatorToPartitionMapper.getInputSplit(
            allocatedEvaluator.getEvaluatorDescriptor().getNodeDescriptor().getName(),
            allocatedEvaluator.getId());

    return ContextConfiguration.CONF
        .set(ContextConfiguration.IDENTIFIER, DATA_LOAD_CONTEXT_PREFIX + numberedSplit.getIndex())
        .build();
  }

  @Override
  public Configuration getServiceConfiguration(final AllocatedEvaluator allocatedEvaluator) {

    try {

      final NumberedSplit<InputSplit> numberedSplit =
          this.evaluatorToPartitionMapper.getInputSplit(
              allocatedEvaluator.getEvaluatorDescriptor().getNodeDescriptor().getName(),
              allocatedEvaluator.getId());

      final Configuration serviceConfiguration = ServiceConfiguration.CONF
          .set(ServiceConfiguration.SERVICES,
              this.inMemory ? InMemoryInputFormatDataSet.class : InputFormatDataSet.class)
          .build();

      return Tang.Factory.getTang().newConfigurationBuilder(serviceConfiguration)
          .bindImplementation(
              DataSet.class,
              this.inMemory ? InMemoryInputFormatDataSet.class : InputFormatDataSet.class)
          .bindNamedParameter(JobConfExternalConstructor.InputFormatClass.class, inputFormatClass)
          .bindNamedParameter(JobConfExternalConstructor.InputPath.class, inputPath)
          .bindNamedParameter(
              InputSplitExternalConstructor.SerializedInputSplit.class,
              WritableSerializer.serialize(numberedSplit.getEntry()))
          .bindConstructor(InputSplit.class, InputSplitExternalConstructor.class)
          .bindConstructor(JobConf.class, JobConfExternalConstructor.class)
          .build();

    } catch (final BindException ex) {
      final String evalId = allocatedEvaluator.getId();
      final String msg = "Unable to create configuration for evaluator " + evalId;
      LOG.log(Level.WARNING, msg, ex);
      throw new RuntimeException(msg, ex);
    }
  }

  @Override
  public String getComputeContextIdPrefix() {
    return COMPUTE_CONTEXT_PREFIX;
  }

  @Override
  public boolean isComputeContext(final ActiveContext context) {
    return context.getId().startsWith(COMPUTE_CONTEXT_PREFIX);
  }

  @Override
  public boolean isDataLoadedContext(final ActiveContext context) {
    return context.getId().startsWith(DATA_LOAD_CONTEXT_PREFIX);
  }
}
