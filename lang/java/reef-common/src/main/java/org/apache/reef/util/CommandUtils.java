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
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * run given command and return the result as string.
 */
public final class CommandUtils {
  /**
   * Standard java logger.
   */
  private static final Logger LOG = Logger.getLogger(CommandUtils.class.getName());

  public static String runCommand(final String command) {
    final StringBuilder sb = new StringBuilder();
    try {
      final String cmd = OSUtils.isWindows() ? "cmd.exe /c " + command : command;
      final Process proc = Runtime.getRuntime().exec(cmd);

      try (BufferedReader input =
               new BufferedReader(new InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8))) {
        String line;
        while ((line = input.readLine()) != null) {
          sb.append(line).append('\n');
        }
      }
    } catch (final IOException ex) {
      LOG.log(Level.SEVERE, "Error in call: " + command, ex);
      sb.append(ex);
    }
    return sb.toString();
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private CommandUtils() {
  }
}
