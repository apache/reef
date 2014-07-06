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

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.tang.annotations.Unit;
import com.microsoft.wake.EventHandler;

/**
 * An EventHandler combines two events of different types into a single Pair of events.
 * Handler will block until both events are received.
 *
 * onNext is thread safe
 *
 * @param <L> type of event
 * @param <R> type of event
 *
 * @see BlockingEventHandler
 */
@Unit
public final class MergingEventHandler<L,R> {

  private static Logger LOG = Logger.getLogger(MergingEventHandler.class.getName());

  private final Object mutex = new Object();

  private final EventHandler<Pair<L,R>> destination;

  private L leftEvent;
  private R rightEvent;

  public final EventHandler<L> left = new Left();
  public final EventHandler<R> right = new Right();

  @Inject
  public MergingEventHandler(final EventHandler<Pair<L, R>> destination) {
    this.destination = destination;
    reset();
  }

  private class Left implements EventHandler<L> {

    @Override
    public void onNext(final L event) {

      L leftRef = null;
      R rightRef = null;

      synchronized (mutex) {

        while (leftEvent != null) {
          try {
            mutex.wait();
          } catch (final InterruptedException e) {
            LOG.log(Level.SEVERE, "Wait interrupted.", e);
          }
        }

        if (LOG.isLoggable(Level.FINEST)) {
          LOG.log(Level.FINEST, "{0} producing left {1}",
                  new Object[] { Thread.currentThread(), event });
        }

        leftEvent = event;
        leftRef = event;

        if (rightEvent != null) {
          rightRef = rightEvent;
          reset();
          mutex.notifyAll();
        }
      }

      if (rightRef != null) {
        // I get to fire the event
        destination.onNext(new Pair<L,R>(leftRef, rightRef));
      }
    }
  }

  private class Right implements EventHandler<R> {

    @Override
    public void onNext(final R event) {

      L leftRef = null;
      R rightRef = null;

      synchronized (mutex) {

        while (rightEvent != null) {
          try {
            mutex.wait();
          } catch (final InterruptedException e) {
            LOG.log(Level.SEVERE, "Wait interrupted.", e);
          }
        }

        if (LOG.isLoggable(Level.FINEST)) {
          LOG.log(Level.FINEST, "{0} producing right {1}",
                  new Object[] { Thread.currentThread(), event });
        }

        rightEvent = event;
        rightRef = event;

        if (leftEvent != null) {
          leftRef = leftEvent;
          reset();
          mutex.notifyAll();
        }
      }

      if (leftRef != null) {
        // I get to fire the event
        destination.onNext(new Pair<L,R>(leftRef, rightRef));
      }
    }
  }

  /*
   * Not thread safe. Must be externally synchronized.
   */
  private void reset() {
    rightEvent = null;
    leftEvent = null;
  }

  public static class Pair<S1,S2> {
    public final S1 first;
    public final S2 second;

    private Pair(S1 s1, S2 s2) {
      this.first = s1;
      this.second = s2;
    }
  }
}
