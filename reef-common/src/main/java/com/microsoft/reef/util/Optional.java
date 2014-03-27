/**
 * Copyright (C) 2013 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.util;

import java.io.Serializable;

/**
 * Represents an optional value. Loosely based on
 * <a href="http://download.java.net/jdk8/docs/api/java/util/Optional.html"></a>The Java 8 version</a>, but filtered for
 * Java 7 compatibility.
 *
 * @param <T>
 */
public final class Optional<T> implements Serializable {

  private static final long serialVersionUID = 42L;
  private final T value;

  private Optional(final T value) {
    this.value = value;
  }

  private Optional() {
    this.value = null;
  }

  /**
   * @param value
   * @param <T>
   * @return An Optional with the given value.
   * @throws NullPointerException if the value is null
   */
  public static <T> Optional<T> of(final T value) throws NullPointerException {
    if (null == value) {
      throw new NullPointerException("Passed a null value. Use ofNullable() instead");
    }
    return new Optional<>(value);
  }

  /**
   * @param <T>
   * @return an Optional with no value.
   */
  public static <T> Optional<T> empty() {
    return new Optional<>();
  }

  /**
   * @param value
   * @param <T>
   * @return An optional representing the given value, or an empty Optional.
   */
  public static <T> Optional<T> ofNullable(final T value) {
    if (null == value) {
      return Optional.empty();
    } else {
      return Optional.of(value);
    }
  }

  /**
   * @return the value represented or null, if isPresent() is false.
   */
  public T get() {
    return this.value;
  }

  /**
   * @param other
   * @return the value of this Optional or other, if no value exists.
   */
  public T orElse(final T other) {
    if (isPresent()) {
      return this.get();
    } else {
      return other;
    }
  }

  /**
   * @return true if there is a value, false otherwise.
   */
  public boolean isPresent() {
    return null != this.value;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    final Optional optional = (Optional) o;

    if (value != null ? !value.equals(optional.value) : optional.value != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return value != null ? value.hashCode() : 0;
  }

  @Override
  public String toString() {
    return "Optional{" +
        "value=" + value +
        '}';
  }
}
