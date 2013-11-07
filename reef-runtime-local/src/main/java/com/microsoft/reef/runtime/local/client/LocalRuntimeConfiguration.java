/**
 * Copyright (C) 2013 Microsoft Corporation
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
import com.microsoft.reef.runtime.common.client.ClientManager;
import com.microsoft.reef.runtime.common.client.RunningJobImpl;
import com.microsoft.reef.runtime.common.client.api.JobSubmissionHandler;
import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationModuleBuilder;
import com.microsoft.tang.formats.OptionalParameter;

import javax.inject.Inject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A ConfigurationModule to configure the local runtime.
 */
public class LocalRuntimeConfiguration extends ConfigurationModuleBuilder {

  public final static class ExecutorServiceConstructor implements ExternalConstructor<ExecutorService> {

    @Inject
    ExecutorServiceConstructor() {
    }

    @Override
    public ExecutorService newInstance() {
      return Executors.newCachedThreadPool();
    }
  }

  @NamedParameter(default_value = "4", doc = "The number of threads to use", short_name = "nThreads")
  public static final class NumberOfThreads implements Name<Integer> {
  }

  @NamedParameter(default_value = "./REEF_LOCAL_RUNTIME", doc = "The folder where logs etc. shall be stored")
  public static class RootFolder implements Name<String> {
  }

  /**
   * The number of threads or processes available to the runtime. This is the upper limit on the number of
   * Evaluators that the local runtime will hand out concurrently. This simulates the size of a physical cluster in
   * terms of the number of slots available on it with one important caveat: The Driver is not counted against this
   * number.
   */
  public static final OptionalParameter<Integer> NUMBER_OF_THREADS = new OptionalParameter<>();

  /**
   * The folder in which the sub-folders, one per Node, will be created. Those will contain one folder per
   * Evaluator instantiated on the virtual node. Those inner folders will be named by the time when the Evaluator was
   * launched.
   */
  public static final OptionalParameter<String> RUNTIME_ROOT_FOLDER = new OptionalParameter<>();

  /**
   * The ConfigurationModule for the local runtime.
   */
  public static final ConfigurationModule CONF = new LocalRuntimeConfiguration()
      .bindImplementation(REEF.class, ClientManager.class)
      .bindImplementation(RunningJob.class, RunningJobImpl.class)
      .bindImplementation(JobSubmissionHandler.class, LocalJobSubmissionHandler.class)
      .bindConstructor(ExecutorService.class, ExecutorServiceConstructor.class)
      .bindNamedParameter(NumberOfThreads.class, NUMBER_OF_THREADS)
      .bindNamedParameter(RootFolder.class, RUNTIME_ROOT_FOLDER)
      .build();


}
