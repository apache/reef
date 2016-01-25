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
package org.apache.reef.tests.statepassing;


import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.Arrays;

/**
 * Task for StatePassingTest.
 */
public class StatePassingTask implements Task {

  private final Counter c;

  @Inject
  public StatePassingTask(final Counter c) {
    this.c = c;
  }


  private static byte[] makeArray(final int size, final byte value) {
    final byte[] result = new byte[size];
    Arrays.fill(result, value);
    return result;
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    this.c.increment();
    return makeArray(this.c.getValue(), (byte) 1);
  }
}
