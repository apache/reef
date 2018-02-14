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
package org.apache.reef.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * OS utils.
 */
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
   * Determines whether the current JVM is running on the Unix-based OS.
   *
   * @return true, if the JVM is running on Linux/Mac. false, otherwise
   */
  public static boolean isUnix() {
    return isLinux() || isMac();
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
   * Determines whether the current JVM is running on the Mac OS.
   *
   * @return true, if the JVM is running on Mac. false, otherwise
   */
  public static boolean isMac() {
    return System.getProperty("os.name").toLowerCase().contains("mac");
  }

  /**
   * @return the process ID of the JVM, if running on Linux/Windows. This returns -1 for other OSs.
   */
  public static long getPID() {
    if (isUnix()) {
      try {
        final Process process = new ProcessBuilder()
            .command("/bin/sh", "-c", "echo $PPID")
            .start();
        final int exitCode = process.waitFor();
        if (exitCode != 0) {
          LOG.log(Level.SEVERE, "Unable to determine PID. Exit code = {0}", exitCode);
          final StringBuilder errorMsg = new StringBuilder();
          try (final BufferedReader reader = new BufferedReader(
              new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8))) {
            for (int i = 0; i < 10 && reader.ready(); ++i) { // Read the first 10 lines from stderr
              errorMsg.append(reader.readLine()).append('\n');
            }
          }
          LOG.log(Level.SEVERE, "Error:\n{0}", errorMsg);
          return -1;
        }
        final byte[] returnBytes = new byte[128];
        if (process.getInputStream().read(returnBytes) == -1) {
          LOG.log(Level.FINE, "No data read because end of stream was reached");
        }
        process.destroy();
        return Long.parseLong(new String(returnBytes, StandardCharsets.UTF_8).trim());
      } catch (final Exception e) {
        LOG.log(Level.SEVERE, "Unable to determine PID", e);
        return -1;
      }
    } else if (isWindows()) {
      try {
        return Long.parseLong(ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
      } catch (final NumberFormatException e) {
        LOG.log(Level.SEVERE, "Unable to determine PID", e);
        return -1;
      }
    } else {
      return -1;
    }
  }

  /**
   * Kill the process.
   *
   * @param pid Process id
   * @throws IOException
   */
  public static void kill(final long pid) throws IOException, InterruptedException {
    if (isUnix()) {
      final Process process = new ProcessBuilder()
          .command("bash", "-c", "kill", "-9", String.valueOf(pid))
          .start();
      final int returnCode = process.waitFor();
      LOG.fine("Kill returned: " + returnCode);
    } else if (isWindows()) {
      final Process process = new ProcessBuilder()
          .command("taskkill.exe", "/f", "/pid", String.valueOf(pid))
          .start();
      final int returnCode = process.waitFor();
      LOG.fine("Kill returned: " + returnCode);
    } else {
      throw new UnsupportedOperationException("Unable to execute kill on unknown OS");
    }
  }

  /**
   * Formats the given variable for expansion by Windows (<code>%VARIABLE%</code>) or Linux (<code>$VARIABLE</code>).
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
