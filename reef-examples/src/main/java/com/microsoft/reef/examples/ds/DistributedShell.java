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
package com.microsoft.reef.examples.ds;

import com.microsoft.reef.client.*;
import com.microsoft.reef.utils.EnvironmentUtils;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Distributed Shell Client.
 */
@Unit
public final class DistributedShell {

  /**
   * Standard java logger.
   */
  private final static Logger LOG = Logger.getLogger(DistributedShell.class.getName());

  /**
   * Reference to the REEF framework.
   */
  private final REEF reef;

  /**
   * List of file resources to be passed to the Evaluators.
   */
  private final List<String> resources;

  /**
   * Result of the Distributed Shell Activity execution on ALL nodes.
   */
  private String dsResult;

  /**
   * Distributed Shell client constructor. Parameters are injected automatically by TANG.
   *
   * @param reef      reference to the REEF framework.
   * @param resources comma-separated list of file resources to be shipped to the evaluators.
   */
  @Inject
  DistributedShell(final REEF reef, @Parameter(DSClient.Files.class) final String resources) {
    this.reef = reef;
    this.resources = DSClient.EMPTY_FILES.equals(resources)
        ? Collections.<String>emptyList() : Arrays.asList(resources.split(":"));
  }

  /**
   * Submits the shell command to be executed on every node of the resource manager.
   *
   * @param cmd shell command to execute.
   * @throws BindException configuration error.
   */
  public void submit(final String cmd) throws BindException {

    final String jobid = "distributed-shell-" + System.currentTimeMillis();
    LOG.log(Level.INFO, "DISTRIBUTED SHELL\n{0} CMD> {1}", new Object[] { jobid, cmd });

    ConfigurationModule driverConf = DriverConfiguration.CONF
        .set(DriverConfiguration.DRIVER_IDENTIFIER, jobid)
        .set(DriverConfiguration.ON_ACTIVITY_COMPLETED, DistributedShellJobDriver.CompletedActivityHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, DistributedShellJobDriver.AllocatedEvaluatorHandler.class)
        .set(DriverConfiguration.ON_DRIVER_STARTED, DistributedShellJobDriver.StartHandler.class)
        .set(DriverConfiguration.ON_DRIVER_STOP, DistributedShellJobDriver.StopHandler.class);

    driverConf = EnvironmentUtils.addClasspath(driverConf, DriverConfiguration.GLOBAL_LIBRARIES);
    driverConf = EnvironmentUtils.addAll(driverConf, DriverConfiguration.GLOBAL_FILES, this.resources);

    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder(driverConf.build());
    cb.bindNamedParameter(DSClient.Command.class, cmd);

    this.reef.submit(cb.build());
  }

  /**
   * Receive message from the DistributedShellJobDriver.
   * There is only one message, which comes at the end of the driver execution
   * and contains shell command output on each node.
   */
  final class JobMessageHandler implements EventHandler<JobMessage> {
    @Override
    public void onNext(final JobMessage message) {
      final ObjectSerializableCodec<String> codec = new ObjectSerializableCodec<>();
      final String msg = codec.decode(message.get());
      LOG.log(Level.INFO, "Got message: {0}", msg);
      synchronized (DistributedShell.this) {
        assert (DistributedShell.this.dsResult == null);
        DistributedShell.this.dsResult = msg;
      }
    }
  }

  /**
   * Receive notification from DistributedShellJobDriver that the job had completed successfully.
   */
  final class CompletedJobHandler implements EventHandler<CompletedJob> {
    @Override
    public void onNext(final CompletedJob job) {
      LOG.log(Level.INFO, "Completed job: {0}", job.getId());
      synchronized (DistributedShell.this) {
        DistributedShell.this.notify();
      }
    }
  }

  /**
   * Wait for the Distributed Shell job to complete and return the result.
   *
   * @return concatenated results from all distributed shells.
   */
  public String getResult() {
    while (this.dsResult == null) {
      LOG.info("Waiting for the Distributed Shell processes to complete.");
      try {
        synchronized (this) {
          this.wait();
        }
      } catch (final InterruptedException ex) {
        LOG.log(Level.WARNING, "Waiting for result interrupted.", ex);
      }
    }
    this.reef.close();
    return this.dsResult;
  }
}
