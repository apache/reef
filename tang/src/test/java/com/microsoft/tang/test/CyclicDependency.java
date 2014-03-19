package com.microsoft.tang.test;

import javax.inject.Inject;

/**
 * Part of a cyclic dependency
 */
final class CyclicDependency {
  private final CyclicDependencyClassOne one;
  private final CyclicDependencyClassTwo two;

  @Inject
  CyclicDependency(final CyclicDependencyClassOne one,
                   final CyclicDependencyClassTwo two) {
    this.one = one;
    this.two = two;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    CyclicDependency that = (CyclicDependency) o;

    if (!one.equals(that.one)) return false;
    if (!two.equals(that.two)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = one.hashCode();
    result = 31 * result + two.hashCode();
    return result;
  }
}
