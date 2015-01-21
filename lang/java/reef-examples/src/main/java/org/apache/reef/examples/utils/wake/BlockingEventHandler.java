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
package org.apache.reef.examples.utils.wake;

import org.apache.reef.wake.EventHandler;

import java.util.ArrayList;
import java.util.List;

/**
 * An EventHandler that blocks until a set number of Events has been received.
 * Once they have been received, the downstream event handler is called with an
 * Iterable of the events spooled.
 *
 * @param <T>
 */
public final class BlockingEventHandler<T> implements EventHandler<T> {

  private final int expectedSize;
  private final EventHandler<Iterable<T>> destination;
  private List<T> events = new ArrayList<>();

  public BlockingEventHandler(final int expectedSize, final EventHandler<Iterable<T>> destination) {
    this.expectedSize = expectedSize;
    this.destination = destination;
  }

  @Override
  public final void onNext(final T event) {
    if (this.isComplete()) {
      throw new IllegalStateException("Received more Events than expected");
    }
    this.events.add(event);
    if (this.isComplete()) {
      this.destination.onNext(events);
      this.reset();
    }
  }

  private boolean isComplete() {
    return this.events.size() >= expectedSize;
  }

  private void reset() {
    this.events = new ArrayList<>();
  }
}
