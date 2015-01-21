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
import org.apache.reef.wake.exception.WakeRuntimeException;

import java.util.Map;

/**
 * Event handler that dispatches an event to a specific handler based on an event class type
 *
 * @param <T> type
 */
public class MultiEventHandler<T> implements EventHandler<T> {

  private final Map<Class<? extends T>, EventHandler<? extends T>> map;

  /**
   * Constructs a multi-event handler
   *
   * @param map a map of class types to event handlers
   */
  public MultiEventHandler(Map<Class<? extends T>, EventHandler<? extends T>> map) {
    this.map = map;
  }

  /**
   * Invokes a specific handler for the event class type if it exists
   *
   * @param an event
   * @throws WakeRuntimeException
   */
  @Override
  public void onNext(T event) {
    EventHandler<T> handler = (EventHandler<T>) map.get(event.getClass());
    if (handler == null)
      throw new WakeRuntimeException("No event " + event.getClass() + " handler");
    handler.onNext(event);
  }

}


