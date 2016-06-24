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
package org.apache.reef.examples.hello;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.runtime.standalone.client.StandaloneRuntimeConfiguration;
import org.apache.reef.runtime.standalone.client.parameters.NodeListFilePath;
import org.apache.reef.runtime.standalone.client.parameters.SshPortNum;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.util.EnvironmentUtils;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Client for Hello REEF example on standalone environment.
 * This can be run with the command: `java -cp lang/java/reef-examples/target/reef-examples-*-SNAPSHOT-shaded.jar
 *     org.apache.reef.examples.hello.HelloREEFStandalone -nodelist ../NodeList.txt -port 22`
 * Here, we assume that the list of nodes is saved in the ../Nodelist.txt file, with each line containing ssh addresses
 * (i.e. `username@147.12.0.16`), and `~/.ssh/id_dsa` is set up on your local, with `~/.ssh/authorized_keys` containing
 * the contents of your `~/.ssh/id_dsa.pub`.
 * The port parameter is optional.
 */
public final class HelloREEFStandalone {
  private static final Logger LOG = Logger.getLogger(HelloREEFStandalone.class.getName());

  /**
   * Number of milliseconds to wait for the job to complete.
   */
  private static final int JOB_TIMEOUT = 10000; // 10 sec.


  /**
   * @return the configuration of the runtime
   */
  private static Configuration getRuntimeConfiguration(final String nodeListFileName, final int sshPortNum) {
    return StandaloneRuntimeConfiguration.CONF
        .set(StandaloneRuntimeConfiguration.NODE_LIST_FILE_PATH, nodeListFileName)
        .set(StandaloneRuntimeConfiguration.SSH_PORT_NUM, sshPortNum)
        .build();
  }

  /**
   * @return the configuration of the HelloREEF driver.
   */
  private static Configuration getDriverConfiguration() {
    return DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(HelloDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "HelloREEFStandalone")
        .set(DriverConfiguration.ON_DRIVER_STARTED, HelloDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, HelloDriver.EvaluatorAllocatedHandler.class)
        .build();
  }

  /**
   * Start Hello REEF job.
   *
   * @param args command line parameters.
   * @throws BindException      configuration error.
   * @throws InjectionException configuration error.
   */
  public static void main(final String[] args) throws BindException, InjectionException {

    final Tang tang = Tang.Factory.getTang();

    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();

    try{
      new CommandLine(cb)
          .registerShortNameOfClass(NodeListFilePath.class)
          .registerShortNameOfClass(SshPortNum.class)
          .processCommandLine(args);
    } catch(final IOException ex) {
      LOG.log(Level.SEVERE, "Missing parameter 'nodelist' or wrong parameter input.");
      throw new RuntimeException("Missing parameter 'nodelist' or wrong parameter input: ", ex);
    }

    final Injector injector = tang.newInjector(cb.build());

    final String nodeListFilePath = injector.getNamedInstance(NodeListFilePath.class);
    final int sshPortNum = injector.getNamedInstance(SshPortNum.class);

    final Configuration runtimeConf = getRuntimeConfiguration(nodeListFilePath, sshPortNum);
    final Configuration driverConf = getDriverConfiguration();

    final LauncherStatus status = DriverLauncher
        .getLauncher(runtimeConf)
        .run(driverConf, JOB_TIMEOUT);
    LOG.log(Level.INFO, "REEF job completed: {0}", status);
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private HelloREEFStandalone() {
  }
}
