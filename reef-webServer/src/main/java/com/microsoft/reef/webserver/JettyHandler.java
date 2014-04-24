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

import org.mortbay.jetty.HttpConnection;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Jetty Event Handler
 */
class JettyHandler extends AbstractHandler {

    /**
     * a map that contains eventHandler's specification and the reference
     */
    private final Map<String, HttpHandler> eventHandlers = new HashMap<>();

    /**
     * Jetty Event Handler
     * @param httpEventHandlers
     */
    public JettyHandler(Set<HttpHandler> httpEventHandlers)
    {
        //TODO handle duplicated case
        for (HttpHandler h : httpEventHandlers) {
            eventHandlers.put(h.getUriSpecification(), h);
        }
    }

    /**
     * handle http request
     * @param target
     * @param request
     * @param response
     * @param i
     * @throws IOException
     * @throws ServletException
     */
    public void handle(
            String target,
            HttpServletRequest request,
            HttpServletResponse response,
            int i)
            throws IOException, ServletException
    {
        Request baseRequest = (request instanceof Request) ? (Request) request :
                HttpConnection.getCurrentConnection().getRequest();

        //call corresponding HttpHandler
        HttpHandler h = eventHandlers.get(target);
        ReefHttpRequest req = new ReefHttpRequest();  //convert from request
        ReefHttpResponse res = new ReefHttpResponse();  //convert from response
        h.onHttpRequest(req, res);

        //sample response
        response.setContentType("text/html;charset=utf-8");
        response.setStatus(HttpServletResponse.SC_OK);
        baseRequest.setHandled(true);
        response.getWriter().println("<h1>Hello World Julia Wang</h1>");
    }
}
