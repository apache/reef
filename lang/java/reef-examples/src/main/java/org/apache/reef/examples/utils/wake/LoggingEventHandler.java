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
package org.apache.reef.examples.utils.wake;

import org.apache.reef.wake.EventHandler;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An EventHandler that logs its events before handing it to a downstream
 * EventHandler.
 *
 * @param <T>
 */
public class LoggingEventHandler<T> implements EventHandler<T> {

  private static final Logger LOG = Logger.getLogger(LoggingEventHandler.class.getName());

  private final EventHandler<T> downstreamEventHandler;
  private final String format;

  /**
   * @param downstreamEventHandler the event handler to hand the event to
   * @param format                 Format string to log the event, e.g. "Event {0} received".
   */
  public LoggingEventHandler(final EventHandler<T> downstreamEventHandler, final String format) {
    this.downstreamEventHandler = downstreamEventHandler;
    this.format = format;
  }

  public LoggingEventHandler(final EventHandler<T> downstreamEventHandler) {
    this(downstreamEventHandler, "{0}");
  }

  @Override
  public void onNext(final T value) {
    LOG.log(Level.INFO, this.format, value);
    this.downstreamEventHandler.onNext(value);
  }
}
