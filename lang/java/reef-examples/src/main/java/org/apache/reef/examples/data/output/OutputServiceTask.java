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
package org.apache.reef.examples.data.output;

import org.apache.reef.io.data.output.OutputStreamProvider;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * The Task code for the output service demo app.
 * This task receives an output stream from the output service
 * and writes "Hello REEF!" on it.
 */
public final class OutputServiceTask implements Task {

  /**
   * Output stream provider object through which tasks create output streams.
   */
  private final OutputStreamProvider outputStreamProvider;

  /**
   * Task constructor - instantiated via TANG.
   *
   * @param outputStreamProvider Output stream provider object through which tasks create output streams.
   */
  @Inject
  public OutputServiceTask(final OutputStreamProvider outputStreamProvider) {
    this.outputStreamProvider = outputStreamProvider;
  }

  /**
   * Receives an output stream from the output service and writes "Hello REEF!" on it.
   *
   * @param memento the memento objected passed down by the driver.
   * @return null
   * @throws java.io.IOException
   */
  @Override
  public byte[] call(final byte[] memento) throws IOException {
    try (DataOutputStream outputStream = outputStreamProvider.create("hello")) {
      outputStream.writeBytes("Hello REEF!");
    }
    return null;
  }
}
