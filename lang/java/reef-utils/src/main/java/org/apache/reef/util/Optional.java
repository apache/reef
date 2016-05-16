/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.util;

import net.jcip.annotations.Immutable;
import net.jcip.annotations.ThreadSafe;

import java.io.Serializable;

/**
 * Represents an optional value. Loosely based on
 * <a href="http://download.java.net/jdk8/docs/api/java/util/Optional.html">The Java 8 version</a>, but filtered for
 * Java 7 compatibility.
 */
@Immutable
@ThreadSafe
public final class Optional<T> implements Serializable {

  private static final long serialVersionUID = 42L;

  private final T value;
  private static final String EMPTY_VALUE_STR = "OptionalvNothing";
  private static final int EMPTY_VALUE_HASH = 0;

  private Optional(final T value) {
    this.value = value;
  }

  private Optional() {
    this.value = null;
  }

  /**
   * @return An Optional with the given value.
   * @throws IllegalArgumentException if the value is null
   */
  public static <T> Optional<T> of(final T value) throws IllegalArgumentException {
    if (null == value) {
      throw new IllegalArgumentException("Passed a null value. Use ofNullable() instead");
    }
    return new Optional<>(value);
  }

  /**
   * @return an Optional with no value.
   */
  public static <T> Optional<T> empty() {
    return new Optional<>();
  }

  /**
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
  public boolean equals(final Object obj) {

    if (this == obj) {
      return true;
    }

    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    final Optional that = (Optional) obj;
    return this.value == that.value || this.value != null && this.value.equals(that.value);
  }

  @Override
  public int hashCode() {
    if (this.isPresent()) {
      return this.value.hashCode();
    } else {
      return EMPTY_VALUE_HASH;
    }
  }

  @Override
  public String toString() {
    if (this.isPresent()) {
      return "Optional:{" + this.value + "}";
    } else {
      return EMPTY_VALUE_STR;
    }
  }
}
