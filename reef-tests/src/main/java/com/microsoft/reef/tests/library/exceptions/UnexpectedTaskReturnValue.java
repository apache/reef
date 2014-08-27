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
package com.microsoft.reef.tests.library.exceptions;

/**
 * Thrown by the Driver in a test if a Task returned an unexpected value.
 */
public final class UnexpectedTaskReturnValue extends RuntimeException {
  private final String expected;
  private final String actual;

  public UnexpectedTaskReturnValue(final String expected, final String actual) {
    this.expected = expected;
    this.actual = actual;
  }

  @Override
  public String toString() {
    return "UnexpectedTaskReturnValue{" +
        "expected='" + expected + '\'' +
        ", actual='" + actual + '\'' +
        '}';
  }
}
