package com.microsoft.tang.test;

/**
 * An interface with a type parameter. This can be found e.g. in REEF EventHandlers.
 *
 * @param <T>
 */
public interface TypedInterface<T> {

  public void process(final T value);

}
