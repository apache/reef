package com.microsoft.tang.test;

import javax.inject.Inject;

public class ListInterfaceImplOne implements ListInterface {

  private final int magicNumber;

  @Override
  public void bMethod() {
  }

  @Inject
  ListInterfaceImplOne() {
    magicNumber = 31;
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
      ListInterfaceImplOne one = (ListInterfaceImplOne) obj;
      if (one.magicNumber != magicNumber) {
        return false;
      }
      return true;
    }
  }

  @Override
  public int hashCode() {
    return magicNumber;
  }
}
