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
package org.apache.reef.io.data.loading.api;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.io.data.loading.impl.DistributedDataSetPartitionSerializer;
import org.apache.reef.io.data.loading.impl.AvroEvaluatorRequestSerializer;
import org.apache.reef.io.data.loading.impl.SingleDataCenterEvaluatorToPartitionStrategy;
import org.apache.reef.io.data.loading.impl.DistributedDataSetPartition;
import org.apache.reef.io.data.loading.impl.InputFormatLoadingService;
import org.apache.reef.io.data.loading.impl.JobConfExternalConstructor;
import org.apache.reef.io.data.loading.impl.MultiDataCenterEvaluatorToPartitionStrategy;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.formats.ConfigurationModule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Builder to create a request to the DataLoadingService.
 */
public final class DataLoadingRequestBuilder
    implements org.apache.reef.util.Builder<Configuration> {

  // constant used in several places.
  private static final int UNINITIALIZED = -1;

  /**
   * @deprecated since 0.12. Should use instead
   *             {@link DataLoadingRequestBuilder#dataRequests}
   */
  @Deprecated
  private int memoryMB = UNINITIALIZED;
  /**
   * @deprecated since 0.12. Should use instead
   *             {@link DataLoadingRequestBuilder#dataRequests}
   */
  @Deprecated
  private int numberOfCores = UNINITIALIZED;
  private int numberOfDesiredSplits = UNINITIALIZED;
  private List<EvaluatorRequest> computeRequests = new ArrayList<>();
  private final List<EvaluatorRequest> dataRequests = new ArrayList<>();
  private boolean inMemory = false;
  private boolean renewFailedEvaluators = true;
  private ConfigurationModule driverConfigurationModule = null;
  private String inputFormatClass;
  /**
   * Single data center loading strategy flag. Allows to specify if the data
   * will be loaded in machines of a single data center or not. By
   * default, is set to true.
   */
  private boolean singleDataCenterStrategy = true;
  /**
   * Distributed dataset that can contain many distributed partitions.
   */
  private DistributedDataSet distributedDataSet;

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

  /**
   * Adds the requests to the compute requests list.
   *
   * @param computeRequests
   *          the compute requests to add
   * @return this
   */
  @SuppressWarnings("checkstyle:hiddenfield")
  public DataLoadingRequestBuilder addComputeRequests(final List<EvaluatorRequest> computeRequests) {
    for (final EvaluatorRequest computeRequest : computeRequests) {
      addComputeRequest(computeRequest);
    }
    return this;
  }

  /**
   * Adds the requests to the data requests list.
   *
   * @param dataRequests
   *          the data requests to add
   * @return this
   */
  @SuppressWarnings("checkstyle:hiddenfield")
  public DataLoadingRequestBuilder addDataRequests(final List<EvaluatorRequest> dataRequests) {
    for (final EvaluatorRequest dataRequest : dataRequests) {
      addDataRequest(dataRequest);
    }
    return this;
  }

  /**
   * Adds a single request to the compute requests list.
   *
   * @param computeRequest
   *          the compute request to add
   * @return this
   */
  public DataLoadingRequestBuilder addComputeRequest(final EvaluatorRequest computeRequest) {
    this.computeRequests.add(computeRequest);
    return this;
  }

  /**
   * Adds a single request to the data requests list.
   *
   * @param dataRequest
   *          the data request to add
   * @return this
   */
  public DataLoadingRequestBuilder addDataRequest(final EvaluatorRequest dataRequest) {
    this.dataRequests.add(dataRequest);
    return this;
  }

  /**
   * Sets the compute request.
   *
   * @deprecated since 0.12. Should use instead
   *             {@link DataLoadingRequestBuilder#addComputeRequest(EvaluatorRequest)}
   *             or {@link DataLoadingRequestBuilder#addComputeRequests(List)}
   * @param computeRequest
   *          the compute request
   * @return this
   */
  @Deprecated
  public DataLoadingRequestBuilder setComputeRequest(final EvaluatorRequest computeRequest) {
    this.computeRequests = new ArrayList<>(Arrays.asList(computeRequest));
    return this;
  }

  @SuppressWarnings("checkstyle:hiddenfield")
  public DataLoadingRequestBuilder loadIntoMemory(final boolean inMemory) {
    this.inMemory = inMemory;
    return this;
  }

  @SuppressWarnings("checkstyle:hiddenfield")
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

  /**
   * Sets the path of the folder where the data is. Internally it constructs a
   * distributed data set with one partition, no splits and the data can be
   * loaded from anywhere.
   *
   * @deprecated since 0.12. Should use instead
   *             {@link DataLoadingRequestBuilder#setDistributedDataSet(DistributedDataSet)}
   * @param inputPath
   *          the input path
   * @return this
   */
  @Deprecated
  public DataLoadingRequestBuilder setInputPath(final String inputPath) {
    final DistributedDataSet dds = new DistributedDataSet();
    dds.addPartition(DistributedDataSetPartition.newBuilder().setPath(inputPath)
        .setLocation(DistributedDataSetPartition.LOAD_INTO_ANY_LOCATION)
        .setDesiredSplits(Integer.valueOf(NumberOfDesiredSplits.DEFAULT_DESIRED_SPLITS)).build());
    this.singleDataCenterStrategy = true;
    this.distributedDataSet = dds;
    return this;
  }

  /**
   * Sets the distributed data set.
   *
   * @param distributedDataSet
   *          the distributed data set
   * @return this
   */
  public DataLoadingRequestBuilder setDistributedDataSet(final DistributedDataSet distributedDataSet) {
    this.distributedDataSet = distributedDataSet;
    this.singleDataCenterStrategy = false;
    return this;
  }

  @Override
  public Configuration build() throws BindException {
    if (this.driverConfigurationModule == null) {
      throw new BindException("Driver Configuration Module is a required parameter.");
    }

    if (this.distributedDataSet == null || this.distributedDataSet.isEmpty()) {
      throw new BindException("Distributed Data Set is a required parameter.");
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

    // if empty, then the user code still uses the deprecated fields.
    // we create a dataLoadRequest object based on them (or their default values)
    if (this.dataRequests.isEmpty()) {
      final int dataMemoryMB = this.memoryMB > 0 ? this.memoryMB : Integer
          .valueOf(DataLoadingEvaluatorMemoryMB.DEFAULT_DATA_MEMORY);
      final int dataCores = this.numberOfCores > 0 ? this.numberOfCores : Integer
          .valueOf(DataLoadingEvaluatorNumberOfCores.DEFAULT_DATA_CORES);
      final EvaluatorRequest defaultDataRequest = EvaluatorRequest.newBuilder().setMemory(dataMemoryMB)
          .setNumberOfCores(dataCores).build();
      this.dataRequests.add(defaultDataRequest);
    } else {
      // if there are dataRequests, make sure the user did not configure the
      // memory or the number of cores (deprecated API), as they will be discarded
      Validate.isTrue(this.numberOfCores == UNINITIALIZED && this.memoryMB == UNINITIALIZED,
          "Should not set number of cores or memory if you added specific data requests");
    }

    // at this point data requests cannot be empty, either we use the one we created based on the
    // deprecated fields, or the ones created by the user
    for (final EvaluatorRequest request : this.dataRequests) {
      jcb.bindSetEntry(DataLoadingDataRequests.class, AvroEvaluatorRequestSerializer.toString(request));
    }

    // compute requests can be empty to maintain compatibility with previous code.
    if (!this.computeRequests.isEmpty()) {
      for (final EvaluatorRequest request : this.computeRequests) {
        jcb.bindSetEntry(DataLoadingComputeRequests.class, AvroEvaluatorRequestSerializer.toString(request));
      }
    }

    jcb.bindNamedParameter(LoadDataIntoMemory.class, Boolean.toString(this.inMemory))
       .bindNamedParameter(JobConfExternalConstructor.InputFormatClass.class, inputFormatClass);

    final Iterator<DistributedDataSetPartition> partitions = this.distributedDataSet.iterator();
    while (partitions.hasNext()) {
      jcb.bindSetEntry(
          DistributedDataSetPartitionSerializer.DistributedDataSetPartitions.class,
          DistributedDataSetPartitionSerializer.serialize(partitions.next()));
    }

    // we do this check for backwards compatibility, if the user defined it
    // wants to use the single data center loading strategy, we bind that implementation.
    if (this.singleDataCenterStrategy) {
      jcb.bindImplementation(EvaluatorToPartitionStrategy.class, SingleDataCenterEvaluatorToPartitionStrategy.class);
    } else {
      // otherwise, we bind the strategy that will allow the user to specify
      // which evaluators can load the different partitions in a multi data center network topology
      jcb.bindImplementation(EvaluatorToPartitionStrategy.class, MultiDataCenterEvaluatorToPartitionStrategy.class);
    }

    return jcb.bindImplementation(DataLoadingService.class, InputFormatLoadingService.class).build();
  }

  @NamedParameter(short_name = "num_splits", default_value = NumberOfDesiredSplits.DEFAULT_DESIRED_SPLITS)
  public static final class NumberOfDesiredSplits implements Name<Integer> {
    static final String DEFAULT_DESIRED_SPLITS = "0";
  }

  @NamedParameter(short_name = "dataLoadingEvaluatorMemoryMB",
      default_value = DataLoadingEvaluatorMemoryMB.DEFAULT_DATA_MEMORY)
  public static final class DataLoadingEvaluatorMemoryMB implements Name<Integer> {
    static final String DEFAULT_DATA_MEMORY = "4096";
  }

  @NamedParameter(short_name = "dataLoadingEvaluatorCore",
      default_value = DataLoadingEvaluatorNumberOfCores.DEFAULT_DATA_CORES)
  public static final class DataLoadingEvaluatorNumberOfCores implements Name<Integer> {
    static final String DEFAULT_DATA_CORES = "1";
  }

  /**
   * @deprecated since 0.12. Should use instead DataLoadingComputeRequests. No
   *             need for the default value anymore, it is handled in the
   *             DataLoader side in order to disambiguate constructors
   */
  @Deprecated
  @NamedParameter
  public static final class DataLoadingComputeRequest implements Name<String> {
  }

  /**
   * Allows to specify a set of compute requests to send to the DataLoader.
   */
  @NamedParameter(doc = "Sets of compute requests to request to the DataLoader, " +
      "i.e. evaluators requests that will not load data")
  static final class DataLoadingComputeRequests implements Name<Set<String>> {
  }

  /**
   * Allows to specify a set of data requests to send to the DataLoader.
   */
  @NamedParameter(doc = "Sets of data requests to request to the DataLoader, " +
      "i.e. evaluators requests that will load data")
  static final class DataLoadingDataRequests implements Name<Set<String>> {
  }

  @NamedParameter(default_value = "false")
  public static final class LoadDataIntoMemory implements Name<Boolean> {
  }
}
