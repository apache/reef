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
import com.microsoft.reef.driver.parameters.ClientCloseHandlers;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class HttpServerReefEventHandler implements HttpHandler {

  private static final Logger LOG = Logger.getLogger(HttpServerReefEventHandler.class.getName());

  private static final String ver = "v1";

  private final ReefEventStateManager reefStateManager;
  private final Set<EventHandler<Void>> clientCloseHandlers;

  /**
   * specification that would match URI request.
   */
  private String uriSpecification = "Reef";

  @Inject
  public HttpServerReefEventHandler(
      final ReefEventStateManager reefStateManager,
      final @Parameter(ClientCloseHandlers.class) Set<EventHandler<Void>> clientCloseHandlers) {
    this.reefStateManager = reefStateManager;
    this.clientCloseHandlers = clientCloseHandlers;
  }

  /**
   * read a file and output it as a String.
   */
  private static String readFile(final String fileName) throws IOException {
    return new String(Files.readAllBytes(Paths.get(fileName)));
  }

  /**
   * @return URI specification for the handler.
   */
  @Override
  public String getUriSpecification() {
    return uriSpecification;
  }

  /**
   * set URI specification.
   */
  public void setUriSpecification(final String s) {
    uriSpecification = s;
  }

  /**
   * Event handler that is called when receiving a http request.
   */
  @Override
  public void onHttpRequest(
      final ParsedHttpRequest parsedHttpRequest,
      final HttpServletResponse response) throws IOException, ServletException {

    LOG.log(Level.INFO, "HttpServerReefEventHandler in webserver onHttpRequest is called: {0}",
        parsedHttpRequest.getRequestUri());

    final String version = parsedHttpRequest.getVersion().toLowerCase();
    final String target = parsedHttpRequest.getTargetEntity().toLowerCase();

    switch (target) {
      case "evaluators": {
        final String queryStr = parsedHttpRequest.getQueryString();
        if (queryStr == null || queryStr.isEmpty()) {
          if (version.equals(ver)) {
            writeEvaluatorsJsonOutput(response);
          } else {
            writeEvaluatorsWebOutput(response);
          }
        } else {
          handleQueries(response, parsedHttpRequest.getQueryMap(), version);
        }
        break;
      }
      case "driver":
        if (version.equals(ver)) {
          writeDriverJsonInformation(response);
        } else {
          writeDriverWebInformation(response);
        }
        break;
      case "close":
        for (final EventHandler<Void> e : clientCloseHandlers) {
          e.onNext(null);
        }
        response.getWriter().println("Enforced closing");
        break;
      case "kill":
        reefStateManager.OnClientKill();
        response.getWriter().println("Killing");
        break;
      case "logfile":
        //TODO: will get file name from query
        final byte[] outputBody = readFile("stderr.txt").getBytes(Charset.forName("UTF-8"));
        response.getOutputStream().write(outputBody);
        break;
      default:
        response.getWriter().println(String.format("Unsupported query for entity: [%s].", target));
    }
  }

  /**
   * handle HTTP queries
   * Example of a query: http://localhost:8080/reef/Evaluators/?id=Node-2-1403225213803&id=Node-1-1403225213712
   */
  private void handleQueries(
      final HttpServletResponse response,
      final Map<String, List<String>> queries,
      final String version) throws IOException {

    LOG.log(Level.INFO, "HttpServerReefEventHandler handleQueries is called");

    for (final Map.Entry<String, List<String>> entry : queries.entrySet()) {
      final String queryTarget = entry.getKey().toLowerCase();

      switch (queryTarget) {
        case "id":
          if (version.equals(ver)) {
            writeEvaluatorInfoJsonOutput(response, entry.getValue());
          } else {
            writeEvaluatorInfoWebOutput(response, entry.getValue());
          }
          break;
        default:
          response.getWriter().println("Unsupported query : " + queryTarget);
          break;
      }
    }
  }

  /**
   * Write Evaluator info as JSON format to HTTP Response.
   */
  private void writeEvaluatorInfoJsonOutput(
      final HttpServletResponse response, final List<String> ids) throws IOException {
    try {
      final EvaluatorInfoSerializer serializer =
          Tang.Factory.getTang().newInjector().getInstance(EvaluatorInfoSerializer.class);
      final AvroEvaluatorsInfo evaluatorsInfo =
          serializer.toAvro(ids, this.reefStateManager.getEvaluators());
      writeResponse(response, serializer.toString(evaluatorsInfo));
    } catch (final InjectionException e) {
      LOG.log(Level.SEVERE, "Error in injecting EvaluatorInfoSerializer.", e);
      writeResponse(response, "Error in injecting EvaluatorInfoSerializer: " + e);
    }
  }

  /**
   * Write Evaluator info on the Response so that to display on web page directly.
   * This is for direct browser queries.
   */
  private void writeEvaluatorInfoWebOutput(
      final HttpServletResponse response, final List<String> ids) throws IOException {

    for (final String id : ids) {

      final EvaluatorDescriptor evaluatorDescriptor = this.reefStateManager.getEvaluators().get(id);
      final PrintWriter writer = response.getWriter();

      if (evaluatorDescriptor != null) {
        final String nodeId = evaluatorDescriptor.getNodeDescriptor().getId();
        final String nodeName = evaluatorDescriptor.getNodeDescriptor().getName();
        final InetSocketAddress address =
            evaluatorDescriptor.getNodeDescriptor().getInetSocketAddress();

        writer.println("Evaluator Id: " + id);
        writer.write("<br/>");
        writer.println("Evaluator Node Id: " + nodeId);
        writer.write("<br/>");
        writer.println("Evaluator Node Name: " + nodeName);
        writer.write("<br/>");
        writer.println("Evaluator InternetAddress: " + address);
        writer.write("<br/>");
        writer.println("Evaluator Memory: " + evaluatorDescriptor.getMemory());
        writer.write("<br/>");
        writer.println("Evaluator Core: " + evaluatorDescriptor.getNumberOfCores());
        writer.write("<br/>");
        writer.println("Evaluator Type: " + evaluatorDescriptor.getType());
        writer.write("<br/>");
      } else {
        writer.println("Incorrect Evaluator Id: " + id);
      }
    }
  }

  /**
   * Get all evaluator ids and send it back to response as JSON.
   */
  private void writeEvaluatorsJsonOutput(final HttpServletResponse response) throws IOException {
    LOG.log(Level.INFO, "HttpServerReefEventHandler getEvaluators is called");
    try {
      final EvaluatorListSerializer serializer =
          Tang.Factory.getTang().newInjector().getInstance(EvaluatorListSerializer.class);
      final AvroEvaluatorList evaluatorList = serializer.toAvro(
          this.reefStateManager.getEvaluators(), this.reefStateManager.getEvaluators().size(),
          this.reefStateManager.getStartTime());
      writeResponse(response, serializer.toString(evaluatorList));
    } catch (final InjectionException e) {
      LOG.log(Level.SEVERE, "Error in injecting EvaluatorListSerializer.", e);
      writeResponse(response, "Error in injecting EvaluatorListSerializer: " + e);
    }
  }

  /**
   * Get all evaluator ids and send it back to response so that can be displayed on web
   *
   * @param response
   * @throws IOException
   */
  private void writeEvaluatorsWebOutput(final HttpServletResponse response) throws IOException {

    LOG.log(Level.INFO, "HttpServerReefEventHandler getEvaluators is called");

    final PrintWriter writer = response.getWriter();

    writer.println("<h1>Evaluators:</h1>");

    for (final Map.Entry<String, EvaluatorDescriptor> entry
        : this.reefStateManager.getEvaluators().entrySet()) {

      final String key = entry.getKey();
      final EvaluatorDescriptor descriptor = entry.getValue();

      writer.println("Evaluator Id: " + key);
      writer.write("<br/>");
      writer.println("Evaluator Name: " + descriptor.getNodeDescriptor().getName());
      writer.write("<br/>");
    }
    writer.write("<br/>");
    writer.println("Total number of Evaluators: " + this.reefStateManager.getEvaluators().size());
    writer.write("<br/>");
    writer.println(String.format("Driver Start Time:[%s]", this.reefStateManager.getStartTime()));
  }

  /**
   * Write Driver Info as JSON string to Response.
   */
  private void writeDriverJsonInformation(final HttpServletResponse response) throws IOException {
    try {
      final DriverInfoSerializer serializer =
          Tang.Factory.getTang().newInjector().getInstance(DriverInfoSerializer.class);
      final AvroDriverInfo driverInfo = serializer.toAvro(
          this.reefStateManager.getDriverEndpointIdentifier(), this.reefStateManager.getStartTime(), this.reefStateManager.getServicesInfo());
      writeResponse(response, serializer.toString(driverInfo));
    } catch (final InjectionException e) {
      LOG.log(Level.SEVERE, "Error in injecting DriverInfoSerializer.", e);
      writeResponse(response, "Error in injecting DriverInfoSerializer: " + e);
    }
  }

  /**
   * Write a String to HTTP Response.
   */
  private void writeResponse(final HttpServletResponse response, final String data) throws IOException {
    final byte[] outputBody = data.getBytes(Charset.forName("UTF-8"));
    response.getOutputStream().write(outputBody);
  }

  /**
   * Get driver information.
   */
  private void writeDriverWebInformation(final HttpServletResponse response) throws IOException {

    LOG.log(Level.INFO, "HttpServerReefEventHandler writeDriverInformation invoked.");

    final PrintWriter writer = response.getWriter();

    writer.println("<h1>Driver Information:</h1>");

    writer.println(String.format("Driver Remote Identifier:[%s]",
        this.reefStateManager.getDriverEndpointIdentifier()));
    writer.write("<br/><br/>");

    writer.println(String.format("Services registered on Driver:"));
    writer.write("<br/><br/>");
    for(final AvroReefServiceInfo service : this.reefStateManager.getServicesInfo()){
      writer.println(String.format("Service: [%s] , Information: [%s]", service.getServiceName(), service.getServiceInfo()));
      writer.write("<br/><br/>");
    }

    writer.println(String.format("Driver Start Time:[%s]", this.reefStateManager.getStartTime()));
  }
}
