package com.microsoft.tang;

public interface Factory<T, U> {
  public T newInstance(U arg);
}
