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
 * Thrown when a test fails on the client side.
 */
public class ClientSideFailure extends RuntimeException {

  public ClientSideFailure() {
  }

  public ClientSideFailure(final String string) {
    super(string);
  }

  public ClientSideFailure(final String string, final Throwable thrwbl) {
    super(string, thrwbl);
  }

  public ClientSideFailure(final Throwable thrwbl) {
    super(thrwbl);
  }
}
