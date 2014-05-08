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
    private static final int MAX_PORT = 9999;

    /**
     * minimum port number range
     */
    private static final int MIN_PORT = 1000;

    /**
     * Jetty server.
     */
    private Server server;

    /**
     * port number used in Jetty Server
     */
    private int port;

    /**
     * Jetty Handler
     */
    private final JettyHandler jettyHandler;

    /**
     * Constructor of HttpServer that wraps Jetty Server
     *
     * @param jettyHandler
     */
    @Inject
    HttpServerImpl(final JettyHandler jettyHandler, @Parameter(PortNumber.class) int port) throws Exception{
        this.jettyHandler = jettyHandler;
        initialize(port);
    }

    /**
     * Ctraete Jetty Server and start it. If the port has conflict, try another randomly generated port number
     * @param p
     * @throws Exception
     */
    private void initialize(final int p) throws Exception {
        this.server = new Server(p); //Jetty server
        this.port = p;
        this.server.setHandler(jettyHandler); //register handler
        try {
            server.start();
            LOG.log(Level.INFO, "Jetty server is running on port {0}", port);
        } catch (BindException e){
            initialize(getNextPort());
        }
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
