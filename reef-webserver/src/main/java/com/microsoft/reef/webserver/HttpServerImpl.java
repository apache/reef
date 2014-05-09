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
final class HttpServerImpl implements HttpServer {
    /**
     * Standard Java logger.
     */
    private static final Logger LOG = Logger.getLogger(HttpServerImpl.class.getName());

    /**
     * maximum port number range
     */
    static final int MAX_PORT = 49151;

    /**
     * minimum port number range
     */
    static final int MIN_PORT = 1024;

    /**
     * max attempts for prot numbers
     */
    private static int maxAttempts = 100;

    /**
     * Jetty server.
     */
    private Server server;

    /**
     * port number used in Jetty Server
     */
    private int port;

    /**
     * port number passed in is the default that will be tried first
     */
    private boolean useDefaultPort = true;

    /**
     * Constructor of HttpServer that wraps Jetty Server
     * @param jettyHandler
     * @param portNumber
     * @throws Exception
     */
    @Inject
    HttpServerImpl(final JettyHandler jettyHandler, @Parameter(PortNumber.class) int portNumber) throws Exception{
        for (int attempt = 1; attempt < maxAttempts; ++attempt) {
            final int port;
            if (useDefaultPort) {
                useDefaultPort = false;
                port = portNumber;
            } else {
                port = getNextPort();
            }
            final Server srv = new Server(port);
            try {
                srv.start();
                synchronized (this) {
                    this.server = srv;
                    this.port = port;
                    LOG.log(Level.INFO, "Jetty Server started with port: {0}", port);
                    break;
                }
            } catch (final BindException ex) {
                LOG.log(Level.WARNING, "Cannot use port: {0}. Will try another", port);
            }
        }
        this.server.setHandler(jettyHandler); //register handler
    }

    /**
     * get a random port number in min and max range
     * @return
     */
    private int getNextPort()
    {
        return MIN_PORT + (int)(Math.random() * ((MAX_PORT - MIN_PORT) + 1));
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
}
