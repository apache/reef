/**
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
package org.apache.reef.runtime.common;

import org.apache.reef.runtime.common.launch.LaunchClass;
import org.apache.reef.runtime.common.launch.REEFErrorHandler;
import org.apache.reef.runtime.common.launch.REEFMessageCodec;
import org.apache.reef.runtime.common.launch.parameters.ClockConfigurationPath;
import org.apache.reef.runtime.common.launch.parameters.ErrorHandlerRID;
import org.apache.reef.runtime.common.launch.parameters.LaunchID;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.util.ThreadLogger;
import org.apache.reef.util.logging.LoggingSetup;
import org.apache.reef.wake.remote.RemoteConfiguration;

import java.io.IOException;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The main entrance point into any REEF process. It is mostly instantiating LaunchClass and calling .run() on it.
 */
public final class Launcher {

  private final static Logger LOG = Logger.getLogger(Launcher.class.getName());

  static {
    LoggingSetup.setupCommonsLogging();
  }

  private Launcher() {
  }

  /**
   * Parse command line options of the launcher.
   *
   * @param args Command line as passed into main().
   * @return TANG configuration object.
   */
  private static Configuration processCommandLine(
      final String[] args) throws BindException, IOException, InjectionException {

    final JavaConfigurationBuilder commandLineBuilder =
        Tang.Factory.getTang().newConfigurationBuilder();

    new CommandLine(commandLineBuilder)
        .registerShortNameOfClass(ClockConfigurationPath.class)
        .registerShortNameOfClass(ErrorHandlerRID.class)
        .registerShortNameOfClass(LaunchID.class)
        .processCommandLine(args);

    return commandLineBuilder
        // Bind the wake error handler
        .bindNamedParameter(RemoteConfiguration.ErrorHandler.class, REEFErrorHandler.class)
        .bindNamedParameter(RemoteConfiguration.ManagerName.class, "REEF_LAUNCHER")
            // Bind the wake codec
        .bindNamedParameter(RemoteConfiguration.MessageCodec.class, REEFMessageCodec.class)
        .build();
  }

  private static void fail(final String msg, final Throwable t) {
    LOG.log(Level.SEVERE, msg, t);
    throw new RuntimeException(msg, t);
  }


  /**
   * Launches a REEF client process (Driver or Evaluator).
   *
   * @param args
   * @throws Exception
   */
  public static void main(final String[] args) {
    LOG.log(Level.FINE, "Launcher started with user name [{0}]", System.getProperty("user.name"));

    LOG.log(Level.FINE, "Launcher started. Assertions are {0} in this process.",
        EnvironmentUtils.areAssertionsEnabled() ? "ENABLED" : "DISABLED");
    Injector injector = null;
    try {
      injector = Tang.Factory.getTang().newInjector(processCommandLine(args));
    } catch (final BindException | IOException | InjectionException e) {
      fail("Error in parsing the command line", e);
    }

    try (final LaunchClass lc = injector.getInstance(LaunchClass.class)) {
      LOG.log(Level.FINE, "Launcher starting");
      lc.run();
      LOG.log(Level.FINE, "Launcher exiting");
    } catch (final Throwable throwable) {
      fail("Unable to run LaunchClass", throwable);
    }

    LOG.log(Level.INFO, "Exiting Launcher.main()");
    if (LOG.isLoggable(Level.FINEST)) {
      LOG.log(Level.FINEST, ThreadLogger.getFormattedThreadList("Threads running after Launcher.close():"));
    }
    System.exit(0);
    if (LOG.isLoggable(Level.FINEST)) {
      LOG.log(Level.FINEST, ThreadLogger.getFormattedThreadList("Threads running after System.exit():"));
    }
  }

  /**
   * Pass values of the properties specified in the propNames array as <code>-D...</code>
   * command line parameters. Currently used only to pass logging configuration to child JVMs processes.
   *
   * @param vargs     List of command line parameters to append to.
   * @param copyNull  create an empty parameter if the property is missing in current process.
   * @param propNames property names.
   */
  public static void propagateProperties(
      final Collection<String> vargs, final boolean copyNull, final String... propNames) {
    for (final String propName : propNames) {
      final String propValue = System.getProperty(propName);
      if (propValue == null || propValue.isEmpty()) {
        if (copyNull) {
          vargs.add("-D" + propName);
        }
      } else {
        vargs.add(String.format("-D%s=%s", propName, propValue));
      }
    }
  }

  /**
   * Same as above, but with copyNull == false by default.
   */
  public static void propagateProperties(
      final Collection<String> vargs, final String... propNames) {
    propagateProperties(vargs, false, propNames);
  }
}
