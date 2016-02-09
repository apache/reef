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
package org.apache.reef.runtime.local.client;

import org.apache.reef.client.parameters.DriverConfigurationProviders;
import org.apache.reef.runtime.common.client.CommonRuntimeConfiguration;
import org.apache.reef.runtime.common.client.DriverConfigurationProvider;
import org.apache.reef.runtime.common.client.api.JobSubmissionHandler;
import org.apache.reef.runtime.common.evaluator.PIDStoreStartHandler;
import org.apache.reef.runtime.common.files.RuntimeClasspathProvider;
import org.apache.reef.runtime.common.parameters.JVMHeapSlack;
import org.apache.reef.runtime.local.LocalClasspathProvider;
import org.apache.reef.runtime.local.client.parameters.MaxNumberOfEvaluators;
import org.apache.reef.runtime.local.client.parameters.RackNames;
import org.apache.reef.runtime.local.client.parameters.RootFolder;
import org.apache.reef.tang.ConfigurationProvider;
import org.apache.reef.tang.formats.*;
import org.apache.reef.wake.time.Clock;

import java.util.concurrent.ExecutorService;

/**
 * A ConfigurationModule to configure the local resourcemanager.
 */
public class LocalRuntimeConfiguration extends ConfigurationModuleBuilder {

  /**
   * The number of threads or processes available to the resourcemanager. This is the upper limit on the number of
   * Evaluators that the local resourcemanager will hand out concurrently.
   * This simulates the size of a physical cluster in terms of the number of slots available on it
   * with one important caveat: The Driver is not counted against this number.
   */
  public static final OptionalParameter<Integer> MAX_NUMBER_OF_EVALUATORS = new OptionalParameter<>();
  /**
   * The folder in which the sub-folders, one per Node, will be created. Those will contain one folder per
   * Evaluator instantiated on the virtual node. Those inner folders will be named by the time when the Evaluator was
   * launched.
   * <p>
   * If none is given, a folder "REEF_LOCAL_RUNTIME" will be created in the local directory.
   */
  public static final OptionalParameter<String> RUNTIME_ROOT_FOLDER = new OptionalParameter<>();

  /**
   * The fraction of the container memory NOT to use for the Java Heap.
   */
  public static final OptionalParameter<Double> JVM_HEAP_SLACK = new OptionalParameter<>();

  /**
   * Configuration provides whose Configuration will be merged into all Driver Configuration.
   */
  public static final OptionalImpl<ConfigurationProvider> DRIVER_CONFIGURATION_PROVIDERS = new OptionalImpl<>();

  /**
   * The rack names that will be available in the local runtime.
   */
  public static final OptionalParameter<String> RACK_NAMES = new OptionalParameter<>();

  /**
   * The ConfigurationModule for the local resourcemanager.
   */
  public static final ConfigurationModule CONF = new LocalRuntimeConfiguration()
      .merge(CommonRuntimeConfiguration.CONF)
          // Bind the local runtime
      .bindImplementation(JobSubmissionHandler.class, LocalJobSubmissionHandler.class)
      .bindImplementation(DriverConfigurationProvider.class, LocalDriverConfigurationProviderImpl.class)
      .bindConstructor(ExecutorService.class, ExecutorServiceConstructor.class)
      .bindImplementation(RuntimeClasspathProvider.class, LocalClasspathProvider.class)
          // Bind parameters of the local runtime
      .bindNamedParameter(MaxNumberOfEvaluators.class, MAX_NUMBER_OF_EVALUATORS)
      .bindNamedParameter(RootFolder.class, RUNTIME_ROOT_FOLDER)
      .bindNamedParameter(JVMHeapSlack.class, JVM_HEAP_SLACK)
      .bindSetEntry(DriverConfigurationProviders.class, DRIVER_CONFIGURATION_PROVIDERS)
      .bindSetEntry(Clock.StartHandler.class, PIDStoreStartHandler.class)
      .bindSetEntry(RackNames.class, RACK_NAMES)
      .build();


}

