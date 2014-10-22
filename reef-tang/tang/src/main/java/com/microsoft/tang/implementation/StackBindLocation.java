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
package com.microsoft.tang.implementation;

import java.util.Arrays;

import com.microsoft.tang.BindLocation;

public class StackBindLocation implements BindLocation {
  final StackTraceElement[] stack;
  public StackBindLocation() {
    StackTraceElement[] stack = new Throwable().getStackTrace();
    if(stack.length != 0) {
      this.stack = Arrays.copyOfRange(stack, 1, stack.length);
    } else {
      this.stack = new StackTraceElement[0];
    }
  }
  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer("[\n");
    for(StackTraceElement e: stack) {
      sb.append(e.toString() + "\n");
    }
    sb.append("]\n");
    return sb.toString();
  }
}
