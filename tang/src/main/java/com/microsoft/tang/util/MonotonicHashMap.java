package com.microsoft.tang.util;

import java.util.HashMap;
import java.util.Map;

public class MonotonicHashMap<T, U> extends HashMap<T, U> {
  private static final long serialVersionUID = 1L;

  @Override
  public U put(T key, U value) {
    U old = super.get(key);
    if (old != null) {
      throw new IllegalArgumentException("Attempt to re-add: [" + key
          + "] old value: " + old + " new value " + value);
    }
    return super.put(key, value);
  }

  @Override
  public void putAll(Map<? extends T, ? extends U> m) {
    for(T t : m.keySet()) {
      if(containsKey(t)) {
        put(t,m.get(t)); // guaranteed to throw.
      }
    }
    for(T t: m.keySet()) {
      put(t,m.get(t));
    }
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
