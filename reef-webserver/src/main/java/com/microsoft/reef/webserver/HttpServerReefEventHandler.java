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

import com.microsoft.reef.driver.evaluator.EvaluatorDescriptor;

import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * HttpServerReefEventHandler
 */
public final class HttpServerReefEventHandler implements HttpHandler {
    /**
     * Standard Java logger.
     */
    private static final Logger LOG = Logger.getLogger(HttpServerReefEventHandler.class.getName());

    /**
     * reference of ReefEventStateManager
     */
    private final ReefEventStateManager reefStateManager;

    /**
     * HttpServerReefEventHandler constructor.
     */
    @Inject
    public HttpServerReefEventHandler(ReefEventStateManager reefStateManager) {
        this.reefStateManager = reefStateManager;
    }

    /**
     * returns URI specification for the handler
     *
     * @return
     */
    @Override
    public String getUriSpecification() {
        return "Reef";
    }

    /**
     * it is called when receiving a http request
     *
     * @param request
     * @param response
     */
    @Override
    public void onHttpRequest(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        LOG.log(Level.INFO, "HttpServerReefEventHandler in webserver onHttpRequest is called: {0}", request.getRequestURI());
        final RequestParser requestParser = new RequestParser(request);
        if (requestParser.getTargetEntity().equalsIgnoreCase("Evaluators")) {
            final String queryStr = requestParser.getQueryString();
            if (queryStr == null || queryStr.length() == 0) {
                getEvaluators(response);
            } else {
                handleQueries(response, requestParser.getQueryMap());
            }
        } else {
            response.getWriter().println("Unsupported query for entity: " + requestParser.getTargetEntity());
        }
    }

    /**
     * handle queries
     * @param response
     * @param queries
     * @throws IOException
     */
    private void handleQueries(HttpServletResponse response, Map<String, String> queries) throws IOException {
        for (Map.Entry<String, String> entry : queries.entrySet()) {
            final String key = entry.getKey();
            final String val = entry.getValue();
            if (key.equalsIgnoreCase("Id")) {
                EvaluatorDescriptor evaluatorDescriptor = reefStateManager.getEvaluators().get(val);
                if (evaluatorDescriptor != null) {
                    final String id = evaluatorDescriptor.getNodeDescriptor().getId();
                    final String name = evaluatorDescriptor.getNodeDescriptor().getName();
                    InetSocketAddress address = evaluatorDescriptor.getNodeDescriptor().getInetSocketAddress();
                    response.getWriter().println("Evaluator Id: " + val);
                    response.getWriter().write("<br/>");
                    response.getWriter().println("Evaluator Node Id: " + id);
                    response.getWriter().write("<br/>");
                    response.getWriter().println("Evaluator Node Name: " + name);
                    response.getWriter().write("<br/>");
                    response.getWriter().println("Evaluator InternetAddress: " + address);
                    response.getWriter().write("<br/>");
                } else {
                    response.getWriter().println("Incorrect Evaluator Id: " + val);
                }
            } else {
                response.getWriter().println("Not supported query string: " + key + "=" + val);
            }
        }
    }

    /**
     * Get all evaluator ids and send it back to response
     *
     * @param response
     * @throws IOException
     */
    private void getEvaluators(HttpServletResponse response) throws IOException {
        response.getWriter().println("<h1>Evaluators:</h1>");

        for (Map.Entry<String, EvaluatorDescriptor> entry : reefStateManager.getEvaluators().entrySet()) {
            final String key = entry.getKey();
            final EvaluatorDescriptor descriptor = entry.getValue();
            response.getWriter().println("Evaluator ID: " + key);
            response.getWriter().write("<br/>");
        }
        response.getWriter().write("<br/>");
        response.getWriter().println("Total number of Evaluators: " + reefStateManager.getEvaluators().size());
        response.getWriter().write("<br/>");
        response.getWriter().println("Start time: " + reefStateManager.getStartTime());
    }
}