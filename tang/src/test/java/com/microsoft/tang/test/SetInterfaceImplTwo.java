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

final class SetInterfaceImplTwo implements SetInterface {

  private final double magicNumber;

  @Inject
  SetInterfaceImplTwo() {
    this.magicNumber = 42.0;
  }

  @Override
  public void aMethod() {

  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    SetInterfaceImplTwo that = (SetInterfaceImplTwo) o;

    if (Double.compare(that.magicNumber, magicNumber) != 0) return false;

    return true;
  }

  @Override
  public int hashCode() {
    long temp = Double.doubleToLongBits(magicNumber);
    return (int) (temp ^ (temp >>> 32));
  }
}
