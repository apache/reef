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
package com.microsoft.reef.runtime.local.client;

import com.microsoft.reef.client.REEF;
import com.microsoft.reef.client.RunningJob;
import com.microsoft.reef.runtime.common.client.REEFImplementation;
import com.microsoft.reef.runtime.common.client.RunningJobImpl;
import com.microsoft.reef.runtime.common.client.api.JobSubmissionHandler;
import com.microsoft.reef.runtime.common.files.RuntimeClasspathProvider;
import com.microsoft.reef.runtime.common.launch.REEFMessageCodec;
import com.microsoft.reef.runtime.common.parameters.JVMHeapSlack;
import com.microsoft.reef.runtime.local.LocalClasspathProvider;
import com.microsoft.reef.runtime.local.client.parameters.NumberOfProcesses;
import com.microsoft.reef.runtime.local.client.parameters.RootFolder;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationModuleBuilder;
import com.microsoft.tang.formats.OptionalParameter;
import com.microsoft.wake.remote.RemoteConfiguration;

import java.util.concurrent.ExecutorService;

/**
 * A ConfigurationModule to configure the local resourcemanager.
 */
public class LocalRuntimeConfiguration extends ConfigurationModuleBuilder {

  /**
   * The number of threads or processes available to the resourcemanager. This is the upper limit on the number of
   * Evaluators that the local resourcemanager will hand out concurrently. This simulates the size of a physical cluster in
   * terms of the number of slots available on it with one important caveat: The Driver is not counted against this
   * number.
   */
  public static final OptionalParameter<Integer> NUMBER_OF_THREADS = new OptionalParameter<>();
  /**
   * The folder in which the sub-folders, one per Node, will be created. Those will contain one folder per
   * Evaluator instantiated on the virtual node. Those inner folders will be named by the time when the Evaluator was
   * launched.
   * <p/>
   * If none is given, a folder "REEF_LOCAL_RUNTIME" will be created in the local directory.
   */
  public static final OptionalParameter<String> RUNTIME_ROOT_FOLDER = new OptionalParameter<>();

  /**
   * The fraction of the container memory NOT to use for the Java Heap.
   */
  public static final OptionalParameter<Double> JVM_HEAP_SLACK = new OptionalParameter<>();

  /**
   * The ConfigurationModule for the local resourcemanager.
   */
  public static final ConfigurationModule CONF = new LocalRuntimeConfiguration()
      .bindImplementation(REEF.class, REEFImplementation.class)
      .bindImplementation(RunningJob.class, RunningJobImpl.class)
      .bindImplementation(JobSubmissionHandler.class, LocalJobSubmissionHandler.class)
      .bindConstructor(ExecutorService.class, ExecutorServiceConstructor.class)
          // Bind the message codec for REEF.
      .bindNamedParameter(RemoteConfiguration.MessageCodec.class, REEFMessageCodec.class)
      .bindNamedParameter(NumberOfProcesses.class, NUMBER_OF_THREADS)
      .bindNamedParameter(RootFolder.class, RUNTIME_ROOT_FOLDER)
      .bindNamedParameter(JVMHeapSlack.class, JVM_HEAP_SLACK)
      .bindImplementation(RuntimeClasspathProvider.class, LocalClasspathProvider.class)
      .build();


}
