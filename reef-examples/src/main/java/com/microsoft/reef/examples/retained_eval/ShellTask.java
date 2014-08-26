/**
 * Copyright (C) 2014 Microsoft Corporation
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

import com.microsoft.reef.task.Task;
import com.microsoft.reef.util.OSUtils;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;

import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Execute command, capture its stdout, and return that string to the job driver.
 */
public class ShellTask implements Task {

  /**
   * Standard java logger.
   */
  private static final Logger LOG = Logger.getLogger(ShellTask.class.getName());

  /**
   * A command to execute.
   */
  private final String command;

  /**
   * Task constructor. Parameters are injected automatically by TANG.
   *
   * @param command a command to execute.
   */
  @Inject
  private ShellTask(@Parameter(Launch.Command.class) final String command) {
    this.command = command;
  }

  /**
   * Execute the shell command and return the result, which is sent back to
   * the JobDriver and surfaced in the CompletedTask object.
   *
   * @param memento ignored.
   * @return byte string containing the stdout from executing the shell command.
   */
  @Override
  public byte[] call(final byte[] memento) {
    final StringBuilder sb = new StringBuilder();
    try {
      // Execute the command
      final String cmd = OSUtils.isWindows() ? "cmd.exe /c " + this.command : this.command;
      LOG.log(Level.FINE, "Call: {0} with: {1}", new Object[]{this.command, memento});
      final Process proc = Runtime.getRuntime().exec(cmd);
      try (final BufferedReader input =
               new BufferedReader(new InputStreamReader(proc.getInputStream()))) {
        String line;
        while ((line = input.readLine()) != null) {
          sb.append(line).append('\n');
        }
      }
    } catch (IOException ex) {
      LOG.log(Level.SEVERE, "Error in call: " + this.command, ex);
      sb.append(ex);
    }
    // Return the result
    final ObjectSerializableCodec<String> codec = new ObjectSerializableCodec<>();
    return codec.encode(sb.toString());
  }
}
