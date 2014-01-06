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
package com.microsoft.reef.runtime.common;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.CommandLine;
import com.microsoft.wake.remote.RemoteConfiguration;

import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Launcher {

  public final static String EVALUATOR_CONFIGURATION_ARG = "runtime_configuration";

  @NamedParameter(doc = "The path to evaluator configuration.", short_name = EVALUATOR_CONFIGURATION_ARG)
  public final static class EvaluatorConfigurationFilePath implements Name<String> {
  }

  public final static String ERROR_HANDLER_RID = "error_handler_rid";

  @NamedParameter(doc = "The error handler remote identifier.", short_name = ERROR_HANDLER_RID)
  public final static class ErrorHandlerRID implements Name<String> {
  }

  public final static String LAUNCH_ID = "launch_id";

  @NamedParameter(doc = "The launch identifier.", short_name = LAUNCH_ID)
  public final static class LaunchID implements Name<String> {
  }

  public final static String[] LOGGING_PROPERTIES = {
      "java.util.logging.config.file",
      "java.util.logging.config.class"
  };

  private final static Logger LOG = Logger.getLogger(Launcher.class.getName());

  /**
   * Logs the currently running threads.
   *
   * @param prefix put before the comma-separated list of threads
   * @param level  the level used for the log entry
   */

  private static void logThreads(final String prefix, final Level level) {
    final StringBuilder sb = new StringBuilder(prefix);
    for (final Thread t : Thread.getAllStackTraces().keySet()) {
      sb.append(t.getName());
      sb.append(", ");
    }
    LOG.log(level, sb.toString());
  }

  /**
   * Parses the command line options of the launcher.
   *
   * @param args
   * @return
   * @throws BindException
   * @throws IOException
   * @throws InjectionException
   */
  private static Configuration processCommandLine(final String[] args) throws BindException, IOException, InjectionException {
    final JavaConfigurationBuilder commandLineBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine cl = new CommandLine(commandLineBuilder);
    cl.registerShortNameOfClass(EvaluatorConfigurationFilePath.class);
    cl.registerShortNameOfClass(ErrorHandlerRID.class);
    cl.registerShortNameOfClass(LaunchID.class);
    cl.processCommandLine(args);
    // Bind the wake error handler
    commandLineBuilder.bindNamedParameter(RemoteConfiguration.ErrorHandler.class, REEFErrorHandler.class);
    commandLineBuilder.bindNamedParameter(RemoteConfiguration.ManagerName.class, "REEF_LAUNCHER");
    // Bind the wake codec
    commandLineBuilder.bindNamedParameter(RemoteConfiguration.MessageCodec.class, REEFMessageCodec.class);
    return commandLineBuilder.build();
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
    LOG.log(Level.FINEST, "Launcher started");
    Injector injector = null;

    try {
      injector = Tang.Factory.getTang().newInjector(processCommandLine(args));
    } catch (final BindException | IOException | InjectionException e) {
      fail("Error in parsing the command line", e);
    }

    LaunchClass lc = null;
    try {
      lc = injector.getInstance(LaunchClass.class);
    } catch (final InjectionException e) {
      fail("Exception in creating the launcher", e);
    }

    lc.run();

    try {
      lc.close();
    } catch (final Exception e) {
      fail("Exception in closing the launcher", e);
    }
    LOG.log(Level.FINEST, "Launcher exiting");
    logThreads("Threads running after Launcher.close(): ", Level.FINEST);
    System.exit(0);
    logThreads("Threads running after System.exit(): ", Level.FINEST);
  }

  /**
   * Pass values of the properties specified in the propNames array as <code>-D...</code>
   * command line parameters. Currently used only to pass logging configuration to child JVMs processes.
   *
   * @param vargs     List of command line parameters to append to.
   * @param propNames Array of property names.
   */
  public static void propagateProperties(final List<String> vargs, final String[] propNames) {
    for (final String propName : propNames) {
      final String propValue = System.getProperty(propName);
      if (!(propValue == null || propValue.isEmpty())) {
        vargs.add(String.format("-D%s=%s", propName, propValue));
      }
    }
  }
}
