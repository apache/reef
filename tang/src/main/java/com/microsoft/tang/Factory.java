package com.microsoft.tang;

public interface Factory<T, U> {
  public T create(U arg);
}
