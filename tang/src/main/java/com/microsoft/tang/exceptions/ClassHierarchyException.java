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
 * This exception is thrown when Tang detects improper or inconsistent class
 * annotations.  This is a runtime exception because it denotes a problem that
 * existed during compilation.
 */
public class ClassHierarchyException extends RuntimeException {
  private static final long serialVersionUID = 1L;
  public ClassHierarchyException(Throwable cause) {
    super(cause);
  }
  public ClassHierarchyException(String msg) {
    super(msg);
  }
  public ClassHierarchyException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
