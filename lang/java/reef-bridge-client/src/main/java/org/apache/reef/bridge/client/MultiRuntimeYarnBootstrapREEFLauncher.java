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

import org.apache.reef.annotations.audience.Interop;
import org.apache.reef.runtime.common.REEFLauncher;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is a bootstrap launcher for YARN for submission of multiruntime jobs from C#. It allows for Java Driver
 * configuration generation directly on the Driver without need of Java dependency if REST
 * submission is used. Note that the name of the class must contain "REEFLauncher" for the time
 * being in order for the Interop code to discover the class.
 */
@Interop(CppFiles = "DriverLauncher.cpp")
public final class MultiRuntimeYarnBootstrapREEFLauncher {
  private static final Logger LOG = Logger.getLogger(MultiRuntimeYarnBootstrapREEFLauncher.class.getName());

  public static void main(final String[] args) throws IOException, InjectionException {
    LOG.log(Level.INFO, "Entering BootstrapLauncher.main().");
    if (args.length != 2) {
      final StringBuilder sb = new StringBuilder();
      sb.append("[ ");
      for (String arg : args) {
        sb.append(arg);
        sb.append(" ");
      }

      sb.append("]");
      final String message = "Bootstrap launcher should have two configuration file inputs, one specifying the" +
          " application submission parameters to be deserialized and the other specifying the job" +
          " submission parameters to be deserialized to create the YarnDriverConfiguration on the fly." +
          " Current args are " + sb.toString();

      throw fatal(message, new IllegalArgumentException(message));
    }

    try {
      final MultiRuntimeYarnBootstrapDriverConfigGenerator driverConfigurationGenerator =
          Tang.Factory.getTang().newInjector().getInstance(MultiRuntimeYarnBootstrapDriverConfigGenerator.class);
      REEFLauncher.main(new String[]{driverConfigurationGenerator.writeDriverConfigurationFile(args[0], args[1])});
    } catch (final Exception exception) {
      if (!(exception instanceof RuntimeException)) {
        throw fatal("Failed to initialize configurations.", exception);
      }

      throw exception;
    }
  }

  private static RuntimeException fatal(final String msg, final Throwable t) {
    LOG.log(Level.SEVERE, msg, t);
    return new RuntimeException(msg, t);
  }

  private MultiRuntimeYarnBootstrapREEFLauncher(){
  }
}
