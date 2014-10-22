package com.microsoft.tang.test;

import javax.inject.Inject;

public class ListInterfaceImplTwo implements ListInterface {
  private final double magicNumber;

  @Override
  public void bMethod() {
  }

  @Inject
  ListInterfaceImplTwo() {
    magicNumber = 31.0;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    else if (obj == null || obj.getClass() != getClass()) {
      return false;
    }
    else {
      ListInterfaceImplTwo two = (ListInterfaceImplTwo) obj;
      if (Double.compare(two.magicNumber, magicNumber) != 0) {
        return false;
      }
      return true;
    }
  }

  @Override
  public int hashCode() {
    long temp = Double.doubleToLongBits(magicNumber);
    return (int) (temp ^ (temp >>> 32));
  }
}
