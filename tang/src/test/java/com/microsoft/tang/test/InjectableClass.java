package com.microsoft.tang.test;

import javax.inject.Inject;

final class InjectableClass {

  private final int magicNumber = -42;

  @Inject
  InjectableClass() {
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    InjectableClass that = (InjectableClass) o;

    if (magicNumber != that.magicNumber) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return magicNumber;
  }
}
