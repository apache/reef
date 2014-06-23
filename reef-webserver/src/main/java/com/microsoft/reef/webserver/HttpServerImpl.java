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

import com.microsoft.tang.annotations.Parameter;
import org.mortbay.jetty.Server;

import javax.inject.Inject;
import java.net.BindException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * HttpServer. It manages Jetty Server and Event Handlers
 */
public final class HttpServerImpl implements HttpServer {
  /**
   * Standard Java logger.
   */
  private static final Logger LOG = Logger.getLogger(HttpServerImpl.class.getName());

  private static JettyHandler jettyHandler;

  /**
   * Jetty server.
   */
  private final Server server;

  /**
   * port number used in Jetty Server
   */
  private final int port;

  /**
   * Constructor of HttpServer that wraps Jetty Server
   *
   * @param jettyHandler
   * @param portNumber
   * @throws Exception
   */
  @Inject
  HttpServerImpl(final JettyHandler jettyHandler,
                 final @Parameter(PortNumber.class) int portNumber,
                 final @Parameter(MaxPortNumber.class) int maxPortNumber,
                 final @Parameter(MinPortNumber.class) int minPortNumber,
                 final @Parameter(MaxRetryAttempts.class) int maxRetryAttempts) throws Exception {
    HttpServerImpl.jettyHandler = jettyHandler;
    int port = portNumber;
    Server srv = null;
    boolean found = false;
    for (int attempt = 0; attempt < maxRetryAttempts; ++attempt) {
      if (attempt > 0) {
        port = getNextPort(maxPortNumber, minPortNumber);
      }
      srv = new Server(port);
      try {
        srv.start();
        found = true;
        break;
      } catch (final BindException ex) {
        LOG.log(Level.FINEST, "Cannot use port: {0}. Will try another", port);
      }
    }

    if (found) {
      this.server = srv;
      this.port = port;
      this.server.setHandler(jettyHandler);
      LOG.log(Level.INFO, "Jetty Server started with port: {0}", port);
    } else {
      throw new RuntimeException("Could not find available port in " + maxRetryAttempts + " attempts");
    }
  }

  /**
   * get a random port number in min and max range
   *
   * @return
   */
  private int getNextPort(final int maxPort, final int minPort) {
    return minPort + (int) (Math.random() * ((maxPort - minPort) + 1));
  }

  /**
   * It will be called from RuntimeStartHandler. As the Jetty server has been started at initialization phase, no need to start here.
   *
   * @throws Exception
   */
  @Override
  public void start() throws Exception {
  }

  /**
   * stop Jetty Server. It will be called from RuntimeStopHandler
   *
   * @throws Exception
   */
  @Override
  public void stop() throws Exception {
    server.stop();
  }

  @Override
  public int getPort() {
    return port;
  }

  /**
   * Add a HttpHandler to Jetty Handler
   *
   * @param httpHandler
   */
  @Override
  public void addHttpHandler(final HttpHandler httpHandler) {
    LOG.log(Level.INFO, "addHttpHandler: {0}", httpHandler.getUriSpecification());
    jettyHandler.addHandler(httpHandler);
  }
}
