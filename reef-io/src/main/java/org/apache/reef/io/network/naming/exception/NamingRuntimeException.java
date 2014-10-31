/**
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
package org.apache.reef.io.network.naming.exception;

import org.apache.reef.io.network.exception.NetworkRuntimeException;

/**
 * Naming resourcemanager exception
 */
public class NamingRuntimeException extends NetworkRuntimeException {
  private static final long serialVersionUID = 1L;

  /**
   * Constructs a new resourcemanager naming exception with the specified detail message and cause
   *
   * @param s the detailed message
   * @param e the cause
   */
  public NamingRuntimeException(String s, Throwable e) {
    super(s, e);
  }

  /**
   * Constructs a new resourcemanager naming exception with the specified detail message
   *
   * @param s the detailed message
   */
  public NamingRuntimeException(String s) {
    super(s);
  }

  /**
   * Constructs a new resourcemanager naming exception with the specified cause
   *
   * @param e the cause
   */
  public NamingRuntimeException(Throwable e) {
    super(e);
  }
}
