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
package com.microsoft.wake.remote.transport.netty;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.microsoft.wake.remote.transport.LinkListener;

/**
 * Link listener that logs a message received
 * 
 * @param <T> type
 */
public class LoggingLinkListener<T> implements LinkListener<T> {

  private static final Logger LOG = Logger.getLogger(LoggingLinkListener.class.getName());

  /**
   * Handles the message received
   * 
   * @param message the message
   */
  @Override
  public void messageReceived(T message) {
    if (LOG.isLoggable(Level.FINEST)) LOG.log(Level.FINEST, "The linklistener " + this.getClass().toString() + "has received " + message);
  }

}
