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
package com.microsoft.tang.test;

import javax.inject.Inject;

/**
 * Created by mweimer on 3/18/14.
 */
final class SetInterfaceImplOne implements SetInterface {

  private final int magicNumber;

  @Inject
  public SetInterfaceImplOne() {
    this.magicNumber = 42;
  }

  @Override
  public void aMethod() {

  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    SetInterfaceImplOne that = (SetInterfaceImplOne) o;

    if (magicNumber != that.magicNumber) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return magicNumber;
  }
}
