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
package org.apache.reef.wake.impl;

import org.apache.reef.wake.EventHandler;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * An EventHandler that blocks until a set number of Events has been received.
 * Once they have been received, the downstream event handler is called with
 * the <i>last event</i> received. The handler resets atomically to start
 * receiving the next batch of events.
 * <p/>
 * onNext is thread safe
 *
 * @param <T> type of events
 * @see BlockingEventHandler
 */
public final class BlockingSignalEventHandler<T> implements EventHandler<T> {

  private final int expectedSize;
  private final EventHandler<T> destination;
  private final AtomicInteger cursor;

  public BlockingSignalEventHandler(final int expectedSize, final EventHandler<T> destination) {
    this.expectedSize = expectedSize;
    this.destination = destination;
    this.cursor = new AtomicInteger(0);
  }

  @Override
  public final void onNext(final T event) {
    int newSize = this.cursor.incrementAndGet();

    if (newSize % expectedSize == 0) {
      this.destination.onNext(event);
    }
  }
}
