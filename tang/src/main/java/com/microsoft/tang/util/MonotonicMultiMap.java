package com.microsoft.tang.util;

public class MonotonicMultiMap<K,V> extends AbstractMonotonicMultiMap<K,V> {
  public MonotonicMultiMap() {
    super(new MonotonicTreeMap<K, java.util.Set<V>>());
  }
}
