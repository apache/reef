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

package org.apache.reef.wake.impl;

import org.apache.reef.wake.MultiObserver;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The MultiObserverImpl class uses reflection to discover which onNext()
 * event processing methods are defined and then map events to them.
 * @param <TSubCls> The subclass derived from MultiObserverImpl.
 */
public abstract class MultiObserverImpl<TSubCls> implements MultiObserver {
  private static final Logger LOG = Logger.getLogger(MultiObserverImpl.class.getName());
  private final Map<String, Method> methodMap = new HashMap<>();

  /**
   * Use reflection to discover all of the event processing methods in TSubCls
   * and setup a means to direct calls from the generic event onNext method defined
   * in the MultiObserver interface to specific concrete event onNext methods.
   */
  public MultiObserverImpl() {
    // Iterate across the methods and build a hash map of class names to reflection methods.
    for (final Method method : this.getClass().getMethods()) {
      if (method.getName().equals("onNext") && method.getDeclaringClass().equals(this.getClass())) {
        // This is an onNext method defined in TSubCls
        final Class<?>[] types = method.getParameterTypes();
        if (types.length == 2 && types[0].getSimpleName().equals("long")) {
          methodMap.put(types[1].getName(), method);
        }
      }
    }
  }

  /**
   * Called when an event is received that does not have an onNext method definition
   * in TSubCls. Override in TSubClas to handle the error.
   * @param event A reference to an object which is an event not handled by TSubCls.
   * @param <TEvent> The type of the event being processed.
   */
  private <TEvent> void unimplemented(final long identifier, final TEvent event) {
    LOG.log(Level.INFO, "Unimplemented event: [" + identifier +"]" + event.getClass().getName());
  }

  /**
   * Generic event onNext method in the base interface which maps the call to a concrete
   * event onNext method in TSubCls if one exists otherwise unimplemented is invoked.
   * @param event An event of type TEvent which will be sent to TSubCls as appropriate.
   * @param <TEvent> The type of the event being processed.
   */
  @Override
  public <TEvent> void onNext(final long identifier, final TEvent event)
    throws IllegalAccessException, InvocationTargetException {

    // Get the reflection method for this call.
    final Method onNext = methodMap.get(event.getClass().getName());
    if (onNext != null) {
      // Process the event.
      onNext.invoke((TSubCls) this, identifier, event);
    } else {
      // Log the unprocessed event.
      unimplemented(identifier, event);
    }
  }
}
