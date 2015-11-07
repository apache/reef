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
package org.apache.reef.bridge.client;

import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runtime.common.REEFLauncher;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The bootstrap launcher that provides 2 ways to Launch a REEF driver, either
 * directly or indirectly. This is generally called through Bridge.exe.
 * The direct flag indicates that the Java Driver configuration is pre-generated, while
 * an indirect method indicates that the Java Driver configuration is generated at
 * runtime. This is to avoid Java dependencies for .NET clients.
 * @see REEFLauncher
 * @see org.apache.reef.runtime.common.launch.LauncherCommandFormatter
 */
@Private
public final class BootstrapLauncher {
  private static final Logger LOG = Logger.getLogger(BootstrapLauncher.class.getName());

  /**
   * Directly calls REEFLauncher with a pre-generated Java driver configuration.
   */
  public static final String DIRECT_LAUNCH = "direct";

  /**
   * Generates a Java driver config for YARN and calls REEFLauncher.
   */
  public static final String YARN_LAUNCH = "yarn";

  public static void main(final String[] args) throws IOException, InjectionException {
    LOG.log(Level.INFO, "Entering BootstrapLauncher.main().");

    if (args.length != 2) {
      final String message = "Bootstrap launcher should have two arguments specifying the launcher runtime and " +
          "the argument configuration to use for the runtime";

      throw fatal(message, new IllegalArgumentException(message));
    }

    try {
      switch (args[0]) {
      case DIRECT_LAUNCH:
        REEFLauncher.main(new String[]{args[1]});
        break;
      case YARN_LAUNCH:
        // TODO[JIRA REEF-922]: Test this path after YARN REST Submission is enabled.
        final YarnBootstrapDriverConfigGenerator yarnDriverConfigurationGenerator =
            Tang.Factory.getTang().newInjector().getInstance(YarnBootstrapDriverConfigGenerator.class);
        REEFLauncher.main(new String[]{yarnDriverConfigurationGenerator.writeDriverConfiguration(args[1])});
        break;
      default:
        throw fatal(args[0] + " is not a valid driver start option.");
      }
    } catch (final Exception exception) {
      if (!(exception instanceof RuntimeException)) {
        throw fatal("Failed to initialize configurations.", exception);
      }

      throw exception;
    }
  }

  private static RuntimeException fatal(final String msg) {
    LOG.log(Level.SEVERE, msg);
    return new RuntimeException(msg);
  }

  private static RuntimeException fatal(final String msg, final Throwable t) {
    LOG.log(Level.SEVERE, msg, t);
    return new RuntimeException(msg, t);
  }

  private BootstrapLauncher(){
  }
}
