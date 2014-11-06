/**
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

/**
 * A reference to a value that can be set exactly once.
 */
public final class SetOnce<T> {

  private Optional<T> value;

  public SetOnce(final T value) {
    this.set(value);
  }

  public SetOnce() {
    this.value = Optional.empty();
  }

  public synchronized T get() {
    return value.get();
  }

  public synchronized void set(final T value) {
    if (this.value.isPresent()) {
      throw new IllegalStateException("Trying to set new value " + value +
          " while an old value was already present: " + this.value);
    }
    this.value = Optional.of(value);
  }

  public synchronized boolean isSet() {
    return this.value.isPresent();
  }
}
