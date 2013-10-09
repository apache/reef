package com.microsoft.tang.util;

import java.util.Map;
import java.util.TreeMap;

public class MonotonicTreeMap<T, U> extends TreeMap<T, U> {
  private static final long serialVersionUID = 1L;

  @Override
  public U put(T key, U value) {
    U old = super.get(key);
    if (old != null) {
      throw new IllegalArgumentException("Attempt to re-add: [" + key
          + "]\n old value: " + old + " new value " + value);
    }
    return super.put(key, value);
  }

  @Override
  public void putAll(Map<? extends T, ? extends U> m) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public U remove(Object o) {
    throw new UnsupportedOperationException();
  }
}