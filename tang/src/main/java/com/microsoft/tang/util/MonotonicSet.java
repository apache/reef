package com.microsoft.tang.util;

import java.util.Collection;
import java.util.HashSet;

public class MonotonicSet<T> extends HashSet<T> {
  private static final long serialVersionUID = 1L;

  @Override
  public boolean add(T e) {
    if (super.contains(e)) {
      throw new IllegalArgumentException("Attempt to re-add " + e
          + " to MonotonicSet!");
    }
    return super.add(e);
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException("Attempt to clear MonotonicSet!");
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException("Attempt to remove " + o
        + " from MonotonicSet!");
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException(
        "removeAll() doesn't make sense for MonotonicSet!");
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException(
        "retainAll() doesn't make sense for MonotonicSet!");
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    throw new UnsupportedOperationException(
        "addAll() not implemennted for MonotonicSet.");
  }
}