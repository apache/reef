package com.microsoft.reef.examples.data.loading;


/**
 * Parses inputs into Examples.
 *
 * @param <T>
 */
public interface Parser<T> {

  public Example parse(final T input);

}
