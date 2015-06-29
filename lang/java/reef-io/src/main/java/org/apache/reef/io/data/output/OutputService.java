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
package org.apache.reef.io.data.output;

import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.evaluator.context.events.ContextStop;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.task.events.TaskStart;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A service class of the output service.
 * The output service provides an output stream,
 * through which tasks write their output to a file
 * without considering the current runtime
 * and collision with other tasks.
 */
@Unit
public final class OutputService {
  private static Logger LOG = Logger.getLogger(OutputService.class.getName());

  /**
   * Output stream provider object through which tasks create output streams.
   */
  private final OutputStreamProvider outputStreamProvider;

  /**
   * Service constructor - instantiated via TANG.
   *
   * @param outputStreamProvider Output stream provider object through which tasks create output streams.
   */
  @Inject
  private OutputService(final OutputStreamProvider outputStreamProvider) {
    this.outputStreamProvider = outputStreamProvider;
  }

  /**
   * Provides a service configuration for the output service.
   *
   * @param outputDir path of the directory where output files are created
   * @param outputStreamProviderClass implementation of OutputStreamProvider
   * @return service configuration.
   */
  public static Configuration getServiceConfiguration(
          final String outputDir,
          final Class<? extends OutputStreamProvider> outputStreamProviderClass) {

    final Configuration partialServiceConf = ServiceConfiguration.CONF
        .set(ServiceConfiguration.SERVICES, outputStreamProviderClass)
        .set(ServiceConfiguration.ON_CONTEXT_STOP, ContextStopHandler.class)
        .set(ServiceConfiguration.ON_TASK_STARTED, TaskStartHandler.class)
        .build();

    return Tang.Factory.getTang()
        .newConfigurationBuilder(partialServiceConf)
        .bindImplementation(OutputStreamProvider.class, outputStreamProviderClass)
        .bindNamedParameter(OutputPath.class, outputDir)
        .build();
  }

  /**
   * Handles the ContextStop event: Close the output stream provider.
   */
  private final class ContextStopHandler implements EventHandler<ContextStop> {
    @Override
    public void onNext(final ContextStop contextStop) {
      LOG.log(Level.INFO, "Context stopped, close the OutputStreamProvider.");
      try {
        outputStreamProvider.close();
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Handles the TaskStart event: Set the task id to the output stream provider.
   */
  private final class TaskStartHandler implements EventHandler<TaskStart> {
    @Override
    public void onNext(final TaskStart taskStart) {
      LOG.log(Level.INFO, String.format("Task %s started, create the OutputStreamProvider.", taskStart.getId()));
      outputStreamProvider.setTaskId(taskStart.getId());
    }
  }

  /**
   * Path of the directory where output files are created.
   */
  @NamedParameter(doc = "Path of the directory where output files are created")
  public final class OutputPath implements Name<String> {
  }
}
