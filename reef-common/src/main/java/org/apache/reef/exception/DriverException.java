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
package org.apache.reef.exception;

import java.util.concurrent.ExecutionException;

/**
 * Thrown by the {@link Driver} and to the clients of {@link REEF}.
 */
public class DriverException extends ExecutionException {

  private static final long serialVersionUID = 1L;

  /**
   * Standard Exception constructor.
   */
  public DriverException() {
    super();
  }

  /**
   * Standard Exception constructor.
   *
   * @param message
   * @param cause
   */
  public DriverException(final String message, final Throwable cause) {
    super(message, cause);
  }

  /**
   * Standard Exception constructor.
   *
   * @param message
   */
  public DriverException(final String message) {
    super(message);
  }

  /**
   * Standard Exception constructor.
   *
   * @param cause
   */
  public DriverException(final Throwable cause) {
    super(cause);
  }

}
