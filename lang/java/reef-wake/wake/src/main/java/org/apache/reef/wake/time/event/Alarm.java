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
package org.apache.reef.wake.time.event;

import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.Time;

/**
 * An alarm is a timer event to be invoked at a given time.
 * Contains a (future) timestamp and the event handler to invoke.
 */
public abstract class Alarm extends Time implements Runnable {

  private final EventHandler<Alarm> handler;

  public Alarm(final long timestamp, final EventHandler<Alarm> handler) {
    super(timestamp);
    this.handler = handler;
  }

  /**
   * Invoke the event handler and pass a reference to self as a parameter.
   */
  @Override
  public final void run() {
    this.handler.onNext(this);
  }
}
