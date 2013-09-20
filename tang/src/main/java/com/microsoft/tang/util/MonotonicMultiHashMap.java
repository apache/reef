package com.microsoft.tang.util;

public class MonotonicMultiHashMap<K,V> extends AbstractMonotonicMultiMap<K,V> {
  public MonotonicMultiHashMap() {
    super(new MonotonicHashMap<K, java.util.Set<V>>());
  }
}
