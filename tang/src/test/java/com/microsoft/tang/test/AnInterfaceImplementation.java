package com.microsoft.tang.test;

import javax.inject.Inject;

final class AnInterfaceImplementation implements AnInterface {
  private final int aMagicNumber;

  @Inject
  AnInterfaceImplementation() {
    this.aMagicNumber = 42;
  }

  @Override
  public void aMethod() {

  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    AnInterfaceImplementation that = (AnInterfaceImplementation) o;

    if (aMagicNumber != that.aMagicNumber) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return aMagicNumber;
  }
}
