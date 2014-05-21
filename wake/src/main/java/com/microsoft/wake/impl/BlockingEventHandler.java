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
package com.microsoft.wake.impl;

import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.microsoft.wake.EventHandler;

/**
 * An EventHandler that blocks until a set number of Events has been received.
 * Once they have been received, the downstream event handler is called with an
 * Iterable of the events spooled.
 * 
 * onNext is thread safe
 * 
 * @param <T> type of events
 *
 * @see BlockingSignalEventHandler
 */
public final class BlockingEventHandler<T> implements EventHandler<T> {

  private final int expectedSize;
  // TODO: a queue is likely overly conservative given that we only need
  // to preserve order of those pairs of events that didn't race (have an ordering)
  private BlockingQueue<T> events = new LinkedBlockingQueue<>();
  private final EventHandler<Iterable<T>> destination;
  private final AtomicInteger cursor;

  public BlockingEventHandler(final int expectedSize, final EventHandler<Iterable<T>> destination) {
    this.expectedSize = expectedSize;
    this.destination = destination;
    this.cursor = new AtomicInteger(0);
  }

  @Override
  public final void onNext(final T event) {
    this.events.add(event);
    int newCursor = this.cursor.incrementAndGet();

    if (newCursor%expectedSize == 0) {
    // FIXME: There is a race here where the person draining the events might 
    // not include their event as the last one. I'm going to assume this does not
    // matter, since all events will still be drained exactly once by someone in
    // the proper order
    
      ArrayList<T> nonConcurrent = new ArrayList<>(expectedSize);
      synchronized (events) {

        // drainTo(maxElements) does not suffice because it has undefined behavior for
        // any modifications (a better spec would possibly be undefined behavior except for appends)
        
        // TODO: a non-locking implementation will simply atomically update the head of the 
        // queue to index=expectedSize, so that the drainer may drain without synchronization
        for (int i=0; i<expectedSize; i++) {
          nonConcurrent.add(events.poll());
        }
      }
      this.destination.onNext(nonConcurrent);
    }
  }
}
