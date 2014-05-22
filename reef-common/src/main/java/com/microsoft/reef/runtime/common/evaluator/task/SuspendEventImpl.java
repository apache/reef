/**
 * Copyright (C) 2014 Microsoft Corporation
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
package com.microsoft.reef.runtime.common.evaluator.task;

import com.microsoft.reef.task.events.SuspendEvent;
import com.microsoft.reef.util.Optional;

final class SuspendEventImpl implements SuspendEvent {
  private final Optional<byte[]> value;

  SuspendEventImpl() {
    this.value = Optional.empty();
  }

  SuspendEventImpl(final byte[] theBytes) {
    this.value = Optional.ofNullable(theBytes);
  }

  @Override
  public Optional<byte[]> get() {
    return this.value;
  }

  @Override
  public String toString() {
    return "SuspendEvent{value=" + this.value + '}';
  }

}
