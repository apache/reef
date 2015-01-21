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
package org.apache.reef.wake.rx.impl;

import org.apache.reef.wake.rx.Observer;
import org.apache.reef.wake.rx.Subject;

import java.util.concurrent.TimeoutException;

public class TimeoutSubject<T> implements Subject<T, T> {
  private Thread timeBomb;
  private Observer<T> destination;
  private boolean finished;

  public TimeoutSubject(final long timeout, final Observer<T> handler) {
    this.finished = false;
    this.destination = handler;
    final TimeoutSubject<T> outer = this;
    this.timeBomb = new Thread(new Runnable() {
      @Override
      public void run() {
        boolean finishedCopy;
        synchronized (outer) {
          if (!finished) {
            try {
              outer.wait(timeout);
            } catch (InterruptedException e) {
              return;
            }
          }
          finishedCopy = finished;
          finished = true; // lock out the caller from putting event through now
        }
        if (!finishedCopy) destination.onError(new TimeoutException("TimeoutSubject expired"));
      }
    });
    this.timeBomb.start();
  }

  @Override
  public void onNext(T value) {
    boolean wasFinished;
    synchronized (this) {
      wasFinished = finished;
      if (!finished) {
        this.notify();
        finished = true;
      }
    }
    if (!wasFinished) {
      // TODO: change Subject to specify conversion to T
      destination.onNext(value);
      destination.onCompleted();
    }
  }

  @Override
  public void onError(Exception error) {
    this.timeBomb.interrupt();
    destination.onError(error);
  }

  @Override
  public void onCompleted() {
    throw new IllegalStateException("Should not be called directly");
  }

}
