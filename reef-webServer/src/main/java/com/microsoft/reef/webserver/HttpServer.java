/**
 * Copyright (C) 2013 Microsoft Corporation
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

import org.mortbay.jetty.Server;
import javax.inject.Inject;
import java.util.Set;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;

/**
 * HttpServer. It manages Jetty Server and Event Handlers
 */
public class HttpServer implements IHttpServer {
    private Set<IHttpHandler> httpEventHandlers;
    private Server server = null;
    private JettyHandler jettyHandler;

    /**
     * Named parameter for HttpEventHanlders
     */
    @NamedParameter
    public class HttpEventHanlders implements Name<Set<IHttpHandler>>
    {
    }

    /**
     * Coostructor of HttpServer. It accepts a set of IHttpHandlers
     * @param httpEventHandlers
     */
    @Inject
    public HttpServer(@Parameter(HttpEventHanlders.class) Set<IHttpHandler> httpEventHandlers)  {
        this.httpEventHandlers = httpEventHandlers;
        jettyHandler = new JettyHandler(httpEventHandlers);
        server = new Server(8080); //Jetty server
        server.setHandler(jettyHandler); //register handler
    }

    /**
     * GetHttpEventHandlers
     * @return
     */
    public Set<IHttpHandler> GetHttpEventHandlers() {
        return  httpEventHandlers;
    }

    /**
     * start Jetty Server. It will be called from RuntimeStartHandler
     * @throws Exception
     */
    @Override
    public void start() throws Exception {
        server.start();
        server.join();
    }

    /**
     * stop Jetty Server. It will be called from RuntimeStopHandler
     * @throws Exception
     */
    @Override
    public void stop() throws Exception {
        server.stop();
    }
}
