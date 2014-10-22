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
package com.microsoft.reef.examples.hellohttp;

import com.microsoft.reef.task.Task;
import com.microsoft.reef.util.CommandUtils;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;

import javax.inject.Inject;
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
   * Object Serializable Codec
   */
  private final ObjectSerializableCodec<String> codec = new ObjectSerializableCodec<>();

  /**
   * Task constructor. Parameters are injected automatically by TANG.
   *
   * @param command a command to execute.
   */
  @Inject
  private ShellTask(@Parameter(Command.class) final String command) {
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
    String result = CommandUtils.runCommand(this.command);
    return codec.encode(result);
  }
}
