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
package com.microsoft.reef.examples.retained_eval;

import com.microsoft.reef.client.ClientConfiguration;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.reef.util.OSUtils;

import com.microsoft.tang.Tang;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.ConfigurationFile;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Distributed Shell end-to-end test.
 */
public class RetainedEvalTest {

  /**
   * Standard Java logger.
   */
  private static final Logger LOG = Logger.getLogger(RetainedEvalTest.class.getName());

  /**
   * Number of worker threads to run.
   */
  private static final int NUM_LOCAL_THREADS = 4;

  /**
   * Message to print in (remote) shells.
   */
  private static final String MESSAGE = "Hello REEF";

  /**
   * TANG configuration object for the remote shell.
   */
  private static Configuration sConfig;

  /**
   * Test class setup - create the configuration object.
   *
   * @throws BindException configuration error.
   */
  @BeforeClass
  public static void setUpClass() throws BindException {

    final JavaConfigurationBuilder confBuilder = Tang.Factory.getTang().newConfigurationBuilder();

    confBuilder.bindNamedParameter(Launch.Local.class, "true");
    confBuilder.bindNamedParameter(Launch.NumEval.class, "" + NUM_LOCAL_THREADS);
    confBuilder.bindNamedParameter(Launch.NumRuns.class, "10");
    confBuilder.bindNamedParameter(Launch.Command.class,
        (OSUtils.isWindows() ? "cmd.exe /C echo " : "echo ") + MESSAGE);

    final Configuration runtimeConfiguration = LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, NUM_LOCAL_THREADS)
        .build();

    final Configuration clientConfiguration = ClientConfiguration.CONF
        .set(ClientConfiguration.ON_JOB_RUNNING, JobClient.RunningJobHandler.class)
        .set(ClientConfiguration.ON_JOB_MESSAGE, JobClient.JobMessageHandler.class)
        .set(ClientConfiguration.ON_JOB_COMPLETED, JobClient.CompletedJobHandler.class)
        .set(ClientConfiguration.ON_JOB_FAILED, JobClient.FailedJobHandler.class)
        .set(ClientConfiguration.ON_RUNTIME_ERROR, JobClient.RuntimeErrorHandler.class)
        .build();

    confBuilder.addConfiguration(runtimeConfiguration);
    confBuilder.addConfiguration(clientConfiguration);

    sConfig = confBuilder.build();

    LOG.log(Level.INFO, "Configuration:\n--\n{0}--",
        ConfigurationFile.toConfigurationString(sConfig));
  }

  /**
   * Test the Distributed Shell in local mode.
   * Run the COMMAND on each worker and make sure the results are as expected.
   *
   * @throws BindException        configuration error.
   * @throws InjectionException   configuration error.
   * @throws InterruptedException waiting for the result interrupted.
   */
  @Test
  public void testDistributedShell() throws BindException, InjectionException {
    final String dsResult = Launch.run(sConfig);
    Assert.assertNotNull(dsResult);
    Assert.assertTrue(dsResult.contains(MESSAGE));
  }
}
