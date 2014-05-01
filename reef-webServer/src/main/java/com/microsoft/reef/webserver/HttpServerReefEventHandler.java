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
     *  reference of ReefEventStateManager
     */
    private final ReefEventStateManager reefStateManager;

    /**
     *  HttpServerReefEventHandler constructor.
     */
    @Inject
    public HttpServerReefEventHandler(ReefEventStateManager reefStateManager) {
        this.reefStateManager = reefStateManager;
    }

    /**
     * returns URI specification for the handler
     * @return
     */
    @Override
    public String getUriSpecification() {
        return "Reef";
    }

    /**
     * it is called when receiving a http request
     * @param request
     * @param response
     */
    @Override
    public void onHttpRequest(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        LOG.log(Level.INFO, "HttpServerReefEventHandler in webserver onHttpRequest is called: " + request.getRequestURI());
        RequestParser requestParser = new RequestParser(request);
        if (requestParser.getTargetEntity().equalsIgnoreCase("Evaluators"))  {
            String queryStr = requestParser.getQuesryString();
            if (queryStr == null || queryStr.length() == 0) {
                getEvaluators(response);
            } else {
                handleQueries(response, queryStr);
            }
        } else {
            response.getWriter().println("Unsupported query for entity: " + requestParser.getTargetEntity());
        }
    }

    /**
     * handle evaluator queries
     * @param response
     * @param queryStr
     * @throws IOException
     */
    private void handleQueries(HttpServletResponse response, String queryStr) throws IOException {
        Map<String, String> queries = new HashMap<>();
        String[] questions = queryStr.split("&");
        for (String s : questions) {
            String [] pair = s.split("=");
            if (pair != null && pair.length == 2) {
                queries.put(pair[0], pair[1]);
            }
            else {
                response.getWriter().println("Incomplete query string " + s);
            }
        }
        for (Map.Entry<String, String> entry : queries.entrySet())  {
            String key = entry.getKey();
            String val = entry.getValue();
            if (key.equalsIgnoreCase("Id")) {
                EvaluatorDescriptor evaluatorDescriptor = reefStateManager.getEvaluators().get(val);
                if (evaluatorDescriptor != null) {
                    response.getWriter().println("Evaluator Id: " + val);
                    response.getWriter().write("<br/>");
                    response.getWriter().println("Evaluator Node Id: " + evaluatorDescriptor.getNodeDescriptor().getId());
                    response.getWriter().write("<br/>");
                    response.getWriter().println("Evaluator Node Name: " + evaluatorDescriptor.getNodeDescriptor().getName());
                    response.getWriter().write("<br/>");
                    response.getWriter().println("Evaluator InternetAddress: " + evaluatorDescriptor.getNodeDescriptor().getInetSocketAddress());
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
     * @param response
     * @throws IOException
     */
    private void getEvaluators(HttpServletResponse response) throws IOException {
        response.getWriter().println("<h1>Evaluators:</h1>");
        int n = 0;
        for (Map.Entry<String, EvaluatorDescriptor> entry : reefStateManager.getEvaluators().entrySet()) {
            String key = entry.getKey();
            EvaluatorDescriptor descriptor = (EvaluatorDescriptor)entry.getValue();
            response.getWriter().print("Evaluator ID: " + key);
            n++;
            response.getWriter().write("<br/>");
        }
        response.getWriter().write("<br/>");
        response.getWriter().println("Total number of Evaluators: " + n);
        response.getWriter().write("<br/>");
        response.getWriter().println("Start time: " + reefStateManager.getStartTime());
    }
}