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
  JettyHandler(final @Parameter(HttpEventHandlers.class) Set<HttpHandler> httpEventHandlers) {
    for (final HttpHandler handler : httpEventHandlers) {
      if (!eventHandlers.containsKey(handler.getUriSpecification())) {
        eventHandlers.put(handler.getUriSpecification().toLowerCase(), handler);
      } else {
        LOG.log(Level.WARNING, "The http event handler for {0} is already registered.", handler.getUriSpecification());
      }
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
      final String target,
      final HttpServletRequest request,
      final HttpServletResponse response,
      final int i)
      throws IOException, ServletException {
    LOG.log(Level.INFO, "JettyHandler handle is entered with target: {0} ", target);

    final Request baseRequest = (request instanceof Request) ? (Request) request :
        HttpConnection.getCurrentConnection().getRequest();

    response.setContentType("text/html;charset=utf-8");

    final ParsedHttpRequest parsedHttpRequest = new ParsedHttpRequest(request);
    final HttpHandler handler = validate(request, response, parsedHttpRequest);
    if (handler != null) {
      LOG.log(Level.INFO, "calling HttpHandler.onHttpRequest from JettyHandler.handle() for {0}.", handler.getUriSpecification());
      handler.onHttpRequest(parsedHttpRequest, response);
      response.setStatus(HttpServletResponse.SC_OK);
    }

    baseRequest.setHandled(true);
    LOG.log(Level.INFO, "JettyHandler handle exists");
  }

  /**
   * Validate request and get http handler for the request
   *
   * @param request
   * @param response
   * @return
   * @throws IOException
   * @throws ServletException
   */
  private HttpHandler validate(final HttpServletRequest request,
                               final HttpServletResponse response,
                               final ParsedHttpRequest parsedHttpRequest) throws IOException, ServletException {
    final String specification = parsedHttpRequest.getTargetSpecification();
    final String version = parsedHttpRequest.getVersion();

    if (specification == null) {
      writeMessage(response, "Specification is not provided in the request.", HttpServletResponse.SC_BAD_REQUEST);
      return null;
    }

    final HttpHandler handler = eventHandlers.get(specification.toLowerCase());
    if (handler == null) {
      writeMessage(response, String.format("No event handler registered for: [%s].", specification), HttpServletResponse.SC_NOT_FOUND);
      return null;
    }

    if (version == null) {
      writeMessage(response, "Version is not provided in the request.", HttpServletResponse.SC_BAD_REQUEST);
      return null;
    }
    return handler;
  }

  /**
   * process write message and status on the response
   *
   * @param response
   * @param message
   * @param status
   * @throws IOException
   */
  private void writeMessage(final HttpServletResponse response, final String message, final int status) throws IOException {
    response.getWriter().println(message);
    response.setStatus(status);
  }

  /**
   * Add a handler explicitly instead of through injection. This is for handlers created on the fly.
   *
   * @param handler
   */
  final void addHandler(final HttpHandler handler) {
    if (handler != null) {
      if (!eventHandlers.containsKey(handler.getUriSpecification().toLowerCase())) {
        eventHandlers.put(handler.getUriSpecification().toLowerCase(), handler);
      } else {
        LOG.log(Level.WARNING, "JettyHandler handle is already registered: {0} ", handler.getUriSpecification());
      }
    }
  }
}
