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
package org.apache.reef.wake.examples.p2p;

import org.apache.reef.wake.EventHandler;

import java.util.LinkedList;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Performs a Pull-to-Push conversion in Wake.
 * <p/>
 * The class pulls from a set of event sources, and pushes to a single
 * EventHandler. If the downstream event handler blocks, this will block,
 * providing a simple rate limiting scheme.
 * <p/>
 * The EventSources are managed in a basic Queue.
 *
 * @param <T> the message type
 */
public final class Pull2Push<T> implements Runnable, AutoCloseable {

  private final EventHandler<T> output; // The downstream EventHandler
  private final Queue<EventSource<T>> sources = new LinkedList<>(); // The upstream event sources
  private boolean closed = false;

  /**
   * @param output the EventHandler that receives the messages from this
   *               Pull2Push.
   */
  public Pull2Push(final EventHandler<T> output) {
    this.output = output;
  }

  /**
   * Registers an event source.
   *
   * @param source The source that will be added to the queue of this
   *               Pull2Push
   */
  public final void register(final EventSource<T> source) {
    this.sources.add(source);
  }

  /**
   * Executes the message loop.
   */
  @Override
  public void run() {

    while (!this.closed) {
      // Grab the next available message source, if any
      final EventSource<T> nextSource = sources.poll();
      if (null != nextSource) {
        // Grab the next message from that source, if any
        final T message = nextSource.getNext();
        if (null != message) {
          // Add the source to the end of the queue again.
          sources.add(nextSource);
          // Send the message. Note that this may block depending on the underlying EventHandler.
          this.output.onNext(message);
        } else {
          // The message source has returned null as the next message. We drop the message source in that case.
          Logger.getLogger(Pull2Push.class.getName()).log(Level.INFO, "Droping message source {0} from the queue", nextSource.toString());
        }
      } else {
        // No source where available. We could put a wait() here.
      }
    }
  }

  @Override
  public void close() throws Exception {
    this.closed = true;
  }
}
