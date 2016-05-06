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
package org.apache.reef.examples.library;

import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;
import org.apache.reef.util.CommandUtils;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Execute command, capture its stdout, and return that string to the job driver.
 */
public final class ShellTask implements Task {

  /**
   * Standard java logger.
   */
  private static final Logger LOG = Logger.getLogger(ShellTask.class.getName());

  /**
   * A command to execute.
   */
  private final String command;

  /**
   * Object Serializable Codec.
   */
  private static final ObjectSerializableCodec<String> CODEC = new ObjectSerializableCodec<>();

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
    LOG.log(Level.INFO, "RUN: command: {0}", this.command);
    final String result = CommandUtils.runCommand(this.command);
    LOG.log(Level.INFO, "RUN: result: {0}", result);
    return CODEC.encode(result);
  }
}
