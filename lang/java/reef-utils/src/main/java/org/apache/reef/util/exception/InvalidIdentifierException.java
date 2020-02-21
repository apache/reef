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

package org.apache.reef.util.exception;

/**
 * Generated when the identifier passed to MultiAsyncToSync.release is
 * invalid.
 */
public final class InvalidIdentifierException extends Exception {

  private final long identifier;

  /**:
   * See java.lang.Exception.
   */
  public InvalidIdentifierException(final long identifier) {
    super("Unknown blocked caller identifier");
    this.identifier = identifier;
  }

  /**
   * Concatenates the invalid identifier to the error message.
   * @return The message string with the invalid identifier.
   */
  public String toString() {
    return String.format("%s = %d", super.toString(), identifier);
  }
}
