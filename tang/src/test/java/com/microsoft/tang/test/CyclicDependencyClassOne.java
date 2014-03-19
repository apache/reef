package com.microsoft.tang.test;

import javax.inject.Inject;

/**
 * Part of a cyclic dependency
 */
final class CyclicDependencyClassOne {
  private final CyclicDependencyClassTwo other;

  @Inject
  CyclicDependencyClassOne(final CyclicDependencyClassTwo other) {
    this.other = other;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    CyclicDependencyClassOne that = (CyclicDependencyClassOne) o;

    if (!other.equals(that.other)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return other.hashCode();
  }
}
