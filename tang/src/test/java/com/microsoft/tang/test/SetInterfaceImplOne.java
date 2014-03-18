package com.microsoft.tang.test;

import javax.inject.Inject;

/**
 * Created by mweimer on 3/18/14.
 */
public class SetInterfaceImplOne implements SetInterface {

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
