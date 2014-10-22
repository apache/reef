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
package com.microsoft.tang.exceptions;

/**
 * Thrown when an illegal or contradictory configuration option is encountered.
 * 
 * While binding configuration values and merging Configuration objects, Tang
 * checks each configuration option to make sure that it is correctly typed,
 * and that it does not override some other setting (even if the two settings
 * bind the same configuration option to the same value).  When a bad
 * configuration option is encountered, a BindException is thrown.
 *
 * @see NameResolutionException which covers the special case where an unknown
 *      configuration option or class is encountered.
 */
public class BindException extends RuntimeException {
  private static final long serialVersionUID = 1L;
  public BindException(String msg, Throwable cause) {
    super(msg,cause);
  }
  public BindException(String msg) {
    super(msg);
  }
}
