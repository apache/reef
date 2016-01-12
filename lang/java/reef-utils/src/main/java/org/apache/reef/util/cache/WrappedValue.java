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
package org.apache.reef.util.cache;

import org.apache.reef.util.Optional;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * A representation of a cached entry.
 * Contains a value and when it was written.
 */
final class WrappedValue<V> {
  private final Callable<V> valueFetcher;
  private final CurrentTime currentTime;

  private Optional<V> value;
  private Optional<Long> writeTime;

  /**
   * Construct a representation of a cached entry.
   * The value is written after computing valueFetcher and the time
   * it was written is recorded using currentTime.
   *
   * @param valueFetcher method used to fetch the value
   * @param currentTime  class that returns the current time
   */
  WrappedValue(final Callable<V> valueFetcher,
                      final CurrentTime currentTime) {
    this.valueFetcher = valueFetcher;
    this.currentTime = currentTime;

    this.value = Optional.empty();
    this.writeTime = Optional.empty();
  }

  public Optional<Long> getWriteTime() {
    return writeTime;
  }

  public Optional<V> getValue() {
    return value;
  }

  /**
   * Must only be called once, by the thread that created this WrappedValue.
   * @return The value returned by valueFetcher
   */
  public synchronized V loadAndGet() throws ExecutionException {
    try {
      value = Optional.ofNullable(valueFetcher.call());
    } catch (final Exception e) {
      throw new ExecutionException(e);
    } finally {
      writeTime = Optional.of(currentTime.now());
      this.notifyAll();
    }
    if (!value.isPresent()) {
      throw new ExecutionException("valueFetcher returned null", new NullPointerException());
    } else {
      return value.get();
    }
  }

  public synchronized V waitAndGet() {
    while (!value.isPresent()) {
      try {
        this.wait();
      } catch (final InterruptedException ignored) {
        // Ignore, as while loop will be re-entered
      }
    }
    return value.get();
  }
}
