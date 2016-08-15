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
package org.apache.reef.runtime.yarn.driver;

import org.apache.reef.webserver.NoOpHttpServerImpl;
import org.apache.reef.webserver.HttpServer;

import javax.inject.Inject;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Http TrackingURLProvider.
 */
public final class HttpTrackingURLProvider implements TrackingURLProvider {
  /**
   * Standard Java logger.
   */
  private static final Logger LOG = Logger.getLogger(HttpTrackingURLProvider.class.getName());

  /**
   * HttpServer.
   */
  private final HttpServer httpServer;

  /**
   * Constructor of HttpTrackingURLProvider.
   *
   * @param httpServer
   */
  @Inject
  public HttpTrackingURLProvider(final HttpServer httpServer) {
    this.httpServer = httpServer instanceof NoOpHttpServerImpl ? null : httpServer;
  }

  /**
   * get tracking URI.
   *
   * @return
   */
  @Override
  public String getTrackingUrl() {
    if (this.httpServer == null) {
      return "";
    }

    try {
      return InetAddress.getLocalHost().getHostAddress() + ":" + httpServer.getPort();
    } catch (final UnknownHostException e) {
      LOG.log(Level.WARNING, "Cannot get host address.", e);
      throw new RuntimeException("Cannot get host address.", e);
    }
  }
}
