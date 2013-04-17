package com.microsoft.tang.util;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.microsoft.tang.BindLocation;
import com.microsoft.tang.implementation.StackBindLocation;

public class TracingMonotonicMap<T, U> implements Map<T,U> {
  private class EntryImpl implements Map.Entry<U, BindLocation> {
    private final U key;
    private final BindLocation value;
    EntryImpl(U key, BindLocation value) {
      this.key = key;
      this.value = value;
    }
    @Override
    public U getKey() {
      return key;
    }

    @Override
    public BindLocation getValue() {
      return value;
    }

    @Override
    public BindLocation setValue(BindLocation value) {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public String toString() {
      return "[" + key + "] set by " + value;
    }
    
  }
  private final MonotonicMap<T, EntryImpl> innerMap;
  public TracingMonotonicMap() {
    innerMap = new MonotonicMap<>();
  }
  @Override
  public void clear() {
    innerMap.clear();
  }
  @Override
  public boolean containsKey(Object key) {
    return innerMap.containsKey(key);
  }
  @Override
  public boolean containsValue(Object value) {
    throw new UnsupportedOperationException();
  }
  @Override
  public Set<java.util.Map.Entry<T, U>> entrySet() {
    throw new UnsupportedOperationException();
  }
  @Override
  public U get(Object key) {
    EntryImpl ret = innerMap.get(key);
    return ret != null ? ret.getKey() : null; 
  }
  @Override
  public boolean isEmpty() {
    return innerMap.isEmpty();
  }
  @Override
  public Set<T> keySet() {
    return innerMap.keySet();
  }
  @Override
  public U put(T key, U value) {
    EntryImpl ret = innerMap.put(key, new EntryImpl(value, new StackBindLocation()));
    return ret != null ? ret.getKey() : null;
  }
  @Override
  public void putAll(Map<? extends T, ? extends U> m) {
    throw new UnsupportedOperationException();
  }
  @Override
  public U remove(Object key) {
    throw new UnsupportedOperationException();
  }
  @Override
  public int size() {
    return innerMap.size();
  }
  @Override
  public Collection<U> values() {
    throw new UnsupportedOperationException();
  }
}
