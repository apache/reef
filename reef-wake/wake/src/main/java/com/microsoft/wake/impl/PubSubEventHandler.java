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

import com.microsoft.wake.EventHandler;
import com.microsoft.wake.exception.WakeRuntimeException;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Event handler that provides publish/subscribe interfaces
 *
 * @param <T> type
 */
public class PubSubEventHandler<T> implements EventHandler<T> {

  private static final Logger LOG = Logger.getLogger(PubSubEventHandler.class.getCanonicalName());
  private final Map<Class<? extends T>, List<EventHandler<? extends T>>> clazzToListOfHandlersMap;
  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  /**
   * Constructs a pub-sub event handler
   */
  public PubSubEventHandler() {
    this.clazzToListOfHandlersMap = new HashMap<Class<? extends T>, List<EventHandler<? extends T>>>();
  }

  /**
   * Constructs a pub-sub event handler with initial subscribed event handlers
   *
   * @param map a map of event class types to lists of event handlers
   */
  public PubSubEventHandler(Map<Class<? extends T>, List<EventHandler<? extends T>>> clazzToListOfHandlersMap) {
    this.clazzToListOfHandlersMap = clazzToListOfHandlersMap;
  }

  /**
   * Subscribes an event handler for an event class type
   *
   * @param clazz   an event class
   * @param handler an event handler
   */
  public void subscribe(Class<? extends T> clazz, EventHandler<? extends T> handler) {
    lock.writeLock().lock();
    try {
      List<EventHandler<? extends T>> list = clazzToListOfHandlersMap.get(clazz);
      if (list == null) {
        list = new LinkedList<EventHandler<? extends T>>();
        clazzToListOfHandlersMap.put(clazz, list);
      }
      list.add(handler);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Invokes subscribed handlers for the event class type
   *
   * @param event an event
   * @throws WakeRuntimeException
   */
  @Override
  public void onNext(T event) {
    LOG.log(Level.FINEST, "Invoked for event: {0}", event);
    lock.readLock().lock();
    List<EventHandler<? extends T>> list;
    try {
      list = clazzToListOfHandlersMap.get(event.getClass());
      if (list == null) {
        throw new WakeRuntimeException("No event " + event.getClass() + " handler");
      }
      for (final EventHandler<? extends T> handler : list) {
        LOG.log(Level.FINEST, "Invoking {0}", handler);
        ((EventHandler<T>) handler).onNext(event);
      }
    } finally {
      lock.readLock().unlock();
    }
  }

}
