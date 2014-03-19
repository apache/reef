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
