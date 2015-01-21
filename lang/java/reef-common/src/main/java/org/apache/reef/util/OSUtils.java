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
package org.apache.reef.util;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class OSUtils {
  private static final Logger LOG = Logger.getLogger(OSUtils.class.getName());

  private OSUtils() {
  }

  /**
   * Determines whether the current JVM is running on the Windows OS.
   *
   * @return true, if the JVM is running on Windows. false, otherwise
   */
  public static boolean isWindows() {
    return System.getProperty("os.name").toLowerCase().contains("windows");
  }

  /**
   * Determines whether the current JVM is running on the Linux OS.
   *
   * @return true, if the JVM is running on Linux. false, otherwise
   */
  public static boolean isLinux() {
    return System.getProperty("os.name").toLowerCase().contains("linux");
  }

  /**
   * @return the process ID of the JVM, if running on Linux. This returns -1 for other OSs.
   */
  public static long getPID() {
    if (isLinux()) {
      try {
        final Process process = new ProcessBuilder()
            .command("bash", "-c", "echo $PPID")
            .start();
        final byte[] returnBytes = new byte[128];
        process.getInputStream().read(returnBytes);
        final Long result = Long.valueOf(new String(returnBytes).trim());
        process.destroy();
        return result;
      } catch (final IOException e) {
        LOG.log(Level.SEVERE, "Unable to determine PID", e);
        return -1;
      }

    } else {
      return -1;
    }
  }

  /**
   * Applies `kill -9` to the process.
   *
   * @param pid
   * @throws IOException
   */
  public static void kill(final long pid) throws IOException, InterruptedException {
    if (isLinux()) {
      final Process process = new ProcessBuilder()
          .command("bash", "-c", "kill", "-9", String.valueOf(pid))
          .start();
      final int returnCode = process.waitFor();
      LOG.fine("Kill returned: " + returnCode);
    } else {
      throw new UnsupportedOperationException("Unable to execute kill on non-linux OS");
    }
  }

  /**
   * Formats the given variable for expansion by Windows (<code>%VARIABE%</code>) or Linux (<code>$VARIABLE</code>)
   *
   * @param variableName
   * @return
   */
  public static String formatVariable(final String variableName) {
    if (isWindows()) {
      return "%" + variableName + "%";
    } else {
      return "$" + variableName;
    }
  }

}
