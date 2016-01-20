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
package org.apache.reef.tang.implementation;

import org.apache.reef.tang.BindLocation;

import java.util.Arrays;

@SuppressWarnings("checkstyle:illegalinstantiation")
public class StackBindLocation implements BindLocation {
  private final StackTraceElement[] stack;

  public StackBindLocation() {
    final StackTraceElement[] stackTrace = new Throwable().getStackTrace();
    if (stackTrace.length != 0) {
      this.stack = Arrays.copyOfRange(stackTrace, 1, stackTrace.length);
    } else {
      this.stack = new StackTraceElement[0];
    }
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("[\n");
    for (final StackTraceElement e : stack) {
      sb.append(e.toString() + "\n");
    }
    sb.append("]\n");
    return sb.toString();
  }
}
