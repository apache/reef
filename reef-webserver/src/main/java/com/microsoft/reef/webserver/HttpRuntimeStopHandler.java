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
package com.microsoft.reef.webserver;

import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.runtime.event.RuntimeStop;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Http Runtime Stop Handler
 */
final class HttpRuntimeStopHandler implements EventHandler<RuntimeStop> {
  /**
   * Standard Java logger.
   */
  private static final Logger LOG = Logger.getLogger(HttpRuntimeStopHandler.class.getName());

  /**
   * HttpServer
   */
  private final HttpServer httpServer;

  /**
   * Constructor of HttpRuntimeStartHandler. It has a reference of HttpServer
   *
   * @param httpServer
   */
  @Inject
  HttpRuntimeStopHandler(final HttpServer httpServer) {
    this.httpServer = httpServer;
  }

  /**
   * Override EventHandler<RuntimeStop>
   *
   * @param runtimeStop
   */
  @Override
  public synchronized void onNext(final RuntimeStop runtimeStop) {
    LOG.log(Level.FINEST, "HttpRuntimeStopHandler: {0}", runtimeStop);
    try {
      httpServer.stop();
      LOG.log(Level.FINEST, "HttpRuntimeStopHandler complete.");
    } catch (final Exception e) {
      LOG.log(Level.SEVERE, "HttpRuntimeStopHandler cannot stop the Server. {0}", e);
    }
  }
}
