package com.microsoft.tang.test;

import com.microsoft.tang.InjectionFuture;

import javax.inject.Inject;

/**
 * Part of a cyclic dependency.
 */
final class CyclicDependencyClassTwo {
  private final InjectionFuture<CyclicDependencyClassOne> other;

  @Inject
  CyclicDependencyClassTwo(InjectionFuture<CyclicDependencyClassOne> other) {
    this.other = other;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return other.hashCode();
  }
}
