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
package org.apache.reef.webserver;

import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.logging.LoggingScope;
import org.apache.reef.util.logging.LoggingScopeFactory;
import org.apache.reef.wake.remote.RemoteConfiguration;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeBegin;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;

import javax.inject.Inject;
import java.net.BindException;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * HttpServer. It manages Jetty Server and Event Handlers
 */
public final class HttpServerImpl implements HttpServer {
  /**
   * Indicates a hostname that isn't set or known.
   */
  public static final String UNKNOWN_HOST_NAME = "##UNKNOWN##";

  /**
   * Standard Java logger.
   */
  private static final Logger LOG = Logger.getLogger(HttpServerImpl.class.getName());

  /**
   *  JettyHandler injected in the constructor.
   */
  private JettyHandler jettyHandler;

  /**
   * Jetty server.
   */
  private final Server server;

  /**
   * port number used in Jetty Server.
   */
  private final int port;

  /**
   * Logging scope factory.
   */
  private final LoggingScopeFactory loggingScopeFactory;

  /**
   * The host name for the HTTPServer.
   */
  private final String hostAddress;

  /**
   * Constructor of HttpServer that wraps Jetty Server.
   *
   * @param jettyHandler
   * @param portNumber
   * @throws Exception
   */
  @Inject
  HttpServerImpl(@Parameter(RemoteConfiguration.HostAddress.class) final String hostAddress,
                 @Parameter(TcpPortRangeBegin.class)final int portNumber,
                 final JettyHandler jettyHandler,
                 final LocalAddressProvider addressProvider,
                 final TcpPortProvider tcpPortProvider,
                 final LoggingScopeFactory loggingScopeFactory) throws Exception {
    this.loggingScopeFactory = loggingScopeFactory;
    this.jettyHandler = jettyHandler;
    int availablePort = portNumber;
    Server srv = null;

    this.hostAddress = UNKNOWN_HOST_NAME.equals(hostAddress) ? addressProvider.getLocalAddress() : hostAddress;
    try (final LoggingScope ls = this.loggingScopeFactory.httpServer()) {

      final Iterator<Integer> ports = tcpPortProvider.iterator();
      while (ports.hasNext() && srv  == null) {
        availablePort = ports.next();
        srv = tryPort(availablePort);
      }

      if (srv  != null) {
        this.server = srv;
        this.port = availablePort;
        this.server.setHandler(jettyHandler);
        LOG.log(Level.INFO, "Jetty Server started with port: {0}", availablePort);
      } else {
        throw new RuntimeException("Could not find available port for http");
      }
    }
  }

  private Server tryPort(final int portNumber) throws Exception {
    Server srv = new Server();
    final Connector connector = new SocketConnector();
    connector.setHost(this.hostAddress);
    connector.setPort(portNumber);
    srv.addConnector(connector);
    try {
      srv.start();
      LOG.log(Level.INFO, "Jetty Server started with port: {0}", portNumber);
    } catch (final BindException ex) {
      srv = null;
      LOG.log(Level.INFO, String.format("Cannot use host:%s,port: %s. Will try another", this.hostAddress, portNumber));
    }
    return srv;
  }

  /**
   * get a random port number in min and max range.
   *
   * @return
   */
  private int getNextPort(final int maxPort, final int minPort) {
    return minPort + (int) (Math.random() * ((maxPort - minPort) + 1));
  }

  /**
   * It will be called from RuntimeStartHandler.
   * As the Jetty server has been started at initialization phase, no need to start here.
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
   * Add a HttpHandler to Jetty Handler.
   *
   * @param httpHandler
   */
  @Override
  public void addHttpHandler(final HttpHandler httpHandler) {
    LOG.log(Level.INFO, "addHttpHandler: {0}", httpHandler.getUriSpecification());
    jettyHandler.addHandler(httpHandler);
  }
}
