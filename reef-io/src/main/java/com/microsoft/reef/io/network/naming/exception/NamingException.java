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
package com.microsoft.reef.io.network.naming.exception;

import com.microsoft.reef.exception.evaluator.NetworkException;

/**
 * Naming exception
 */
public class NamingException extends NetworkException {
  private static final long serialVersionUID = 1L;

  /**
   * Constructs a new resourcemanager naming exception with the specified detail message and cause
   *
   * @param s the detailed message
   * @param e the cause
   */
  public NamingException(String s, Throwable e) {
    super(s, e);
  }

  /**
   * Constructs a new resourcemanager naming exception with the specified detail message
   *
   * @param s the detailed message
   */
  public NamingException(String s) {
    super(s);
  }

  /**
   * Constructs a new resourcemanager naming exception with the specified cause
   *
   * @param e the cause
   */
  public NamingException(Throwable e) {
    super(e);
  }
}
