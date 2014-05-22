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
package com.microsoft.reef.examples.utils.wake;

import com.microsoft.wake.EventHandler;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An EventHandler that logs its events before handing it to a downstream
 * EventHandler.
 *
 * @param <T>
 */
public class LoggingEventHandler<T> implements EventHandler<T> {

  private final EventHandler<T> downstreamEventHandler;
  private final String prefix;
  private final String suffix;

  /**
   * @param prefix                 to be logged before the event
   * @param downstreamEventHandler the event handler to hand the event to
   * @param suffix                 to be logged after the event
   */
  public LoggingEventHandler(final String prefix, EventHandler<T> downstreamEventHandler, final String suffix) {
    this.downstreamEventHandler = downstreamEventHandler;
    this.prefix = prefix;
    this.suffix = suffix;
  }

  public LoggingEventHandler(final EventHandler<T> downstreamEventHandler) {
    this("", downstreamEventHandler, "");
  }

  @Override
  public void onNext(final T value) {
    Logger.getLogger(LoggingEventHandler.class.getName()).log(Level.INFO, prefix + value.toString() + suffix);
    this.downstreamEventHandler.onNext(value);
  }
}
