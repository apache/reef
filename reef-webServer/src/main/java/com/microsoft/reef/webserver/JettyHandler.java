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
import org.mortbay.jetty.HttpConnection;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.handler.AbstractHandler;

import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Jetty Event Handler
 */
class JettyHandler extends AbstractHandler {
    private static final Logger LOG = Logger.getLogger(JettyHandler.class.getName());
    /**
     * a map that contains eventHandler's specification and the reference
     */
    private final Map<String, HttpHandler> eventHandlers = new HashMap<>();

    /**
     * Jetty Event Handler. It accepts a set of IHttpHandlers
     *
     * @param httpEventHandlers
     */
    @Inject
    JettyHandler(@Parameter(HttpEventHandlers.class) Set<HttpHandler> httpEventHandlers) {
        //TODO handle duplicated case
        for (HttpHandler h : httpEventHandlers) {
            eventHandlers.put(h.getUriSpecification().toLowerCase(), h);
        }
    }

    /**
     * handle http request
     *
     * @param target
     * @param request
     * @param response
     * @param i
     * @throws IOException
     * @throws ServletException
     */
    @Override
    public void handle(
            String target,
            HttpServletRequest request,
            HttpServletResponse response,
            int i)
            throws IOException, ServletException {
        LOG.log(Level.INFO, "JettyHandler handle is entered with target: " + target);
        Request baseRequest = (request instanceof Request) ? (Request) request :
                HttpConnection.getCurrentConnection().getRequest();

        response.setContentType("text/html;charset=utf-8");
        response.setStatus(HttpServletResponse.SC_OK);

        RequestParser requestParser = new RequestParser(request);
        //call corresponding HttpHandler
        final HttpHandler h = eventHandlers.get(requestParser.getTargetSpecification().toLowerCase());
        if (h != null) {
            LOG.log(Level.INFO, "calling HttpHandler.onHttpRequest from JettyHandler.handle() for " + h.getUriSpecification());
            h.onHttpRequest(request, response);
        } else {
            response.getWriter().println("HttpHandler is not provided for " + requestParser.getTargetSpecification());
        }

        baseRequest.setHandled(true);
        LOG.log(Level.INFO, "JettyHandler handle exists");
    }
}
