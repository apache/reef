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

import org.apache.reef.driver.ProgressProvider;
import org.apache.reef.driver.evaluator.EvaluatorDescriptor;
import org.apache.reef.driver.parameters.ClientCloseHandlers;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.logging.LogLevelName;
import org.apache.reef.util.logging.LogParser;
import org.apache.reef.util.logging.LoggingScopeFactory;
import org.apache.reef.util.logging.LoggingScopeImpl;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Http handler for REEF events.
 */
public final class HttpServerReefEventHandler implements HttpHandler {

  private static final Logger LOG = Logger.getLogger(HttpServerReefEventHandler.class.getName());

  private static final String VER = "v1";
  private final String driverStdoutFile;
  private final String driverStderrFile;

  private final ReefEventStateManager reefStateManager;
  private final Set<EventHandler<Void>> clientCloseHandlers;
  private final LoggingScopeFactory loggingScopeFactory;
  private final InjectionFuture<ProgressProvider> progressProvider;

  /**
   * Log level string prefix in the log lines.
   */
  private final String logLevelPrefix;

  /**
   * specification that would match URI request.
   */
  private String uriSpecification = "Reef";

  @Inject
  public HttpServerReefEventHandler(
      final ReefEventStateManager reefStateManager,
      @Parameter(ClientCloseHandlers.class) final Set<EventHandler<Void>> clientCloseHandlers,
      @Parameter(LogLevelName.class) final String logLevel,
      final LoggingScopeFactory loggingScopeFactory,
      final REEFFileNames reefFileNames,
      final InjectionFuture<ProgressProvider> progressProvider) {
    this.reefStateManager = reefStateManager;
    this.clientCloseHandlers = clientCloseHandlers;
    this.loggingScopeFactory = loggingScopeFactory;
    this.logLevelPrefix = new StringBuilder().append(logLevel).append(": ").toString();
    this.progressProvider = progressProvider;
    driverStdoutFile = reefFileNames.getDriverStdoutFileName();
    driverStderrFile = reefFileNames.getDriverStderrFileName();
  }

  /**
   * read a file and output it as a String.
   */
  private static String readFile(final String fileName) throws IOException {
    return new String(Files.readAllBytes(Paths.get(fileName)), StandardCharsets.UTF_8);
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
        if (version.equals(VER)) {
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
      if (version.equals(VER)) {
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
      reefStateManager.onClientKill();
      response.getWriter().println("Killing");
      break;
    case "duration":
      final ArrayList<String> lines =
          LogParser.getFilteredLinesFromFile(driverStderrFile, LoggingScopeImpl.DURATION, LoggingScopeImpl.TOKEN, null);
      writeLines(response, lines, "Performance...");
      break;
    case "stages":
      final ArrayList<String> starts =
          LogParser.getFilteredLinesFromFile(driverStderrFile, LoggingScopeImpl.START_PREFIX, logLevelPrefix, null);
      final ArrayList<String> exits =
          LogParser.getFilteredLinesFromFile(driverStderrFile, LoggingScopeImpl.EXIT_PREFIX, logLevelPrefix,
              LoggingScopeImpl.DURATION);
      final ArrayList<String> startsStages = LogParser.findStages(starts, LogParser.START_INDICATORS);
      final ArrayList<String> endStages = LogParser.findStages(exits, LogParser.END_INDICATORS);
      final ArrayList<String> result = LogParser.mergeStages(startsStages, endStages);
      writeLines(response, result, "Current Stages...");
      break;
    case "logfile":
      final List<String> names = parsedHttpRequest.getQueryMap().get("filename");
      final PrintWriter writer = response.getWriter();
      if (names == null || names.size() == 0) {
        writer.println("File name is not provided");
      } else {
        final String fileName = names.get(0);
        if (!fileName.equals(driverStdoutFile) && !fileName.equals(driverStderrFile)) {
          writer.println(String.format("Unsupported file names: [%s] ", fileName));
        } else {
          try {
            final byte[] outputBody = readFile(fileName).getBytes(StandardCharsets.UTF_8);
            writer.print(Arrays.toString(outputBody));
          } catch (final IOException e) {
            writer.println(String.format("Cannot find the log file: [%s].", fileName));
          }
        }
      }
      break;
    case "progress":
      response.getWriter().println(progressProvider.get().getProgress());
      break;
    default:
      response.getWriter().println(String.format("Unsupported query for entity: [%s].", target));
    }
  }

  /**
   * handle HTTP queries.
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
        if (version.equals(VER)) {
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
        writer.println("Evaluator Type: " + evaluatorDescriptor.getProcess());
        writer.write("<br/>");
        writer.println("Evaluator Runtime Name: " + evaluatorDescriptor.getRuntimeName());
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
    LOG.log(Level.INFO, "HttpServerReefEventHandler writeEvaluatorsJsonOutput is called");
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
   * Get all evaluator ids and send it back to response so that can be displayed on web.
   *
   * @param response
   * @throws IOException
   */
  private void writeEvaluatorsWebOutput(final HttpServletResponse response) throws IOException {

    LOG.log(Level.INFO, "HttpServerReefEventHandler writeEvaluatorsWebOutput is called");

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

    LOG.log(Level.INFO, "HttpServerReefEventHandler writeDriverJsonInformation invoked.");

    try {
      final DriverInfoSerializer serializer =
          Tang.Factory.getTang().newInjector().getInstance(DriverInfoSerializer.class);
      final AvroDriverInfo driverInfo = serializer.toAvro(
          this.reefStateManager.getDriverEndpointIdentifier(), this.reefStateManager.getStartTime(),
          this.reefStateManager.getServicesInfo());
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
    final byte[] outputBody = data.getBytes(StandardCharsets.UTF_8);
    response.getOutputStream().write(outputBody);
  }

  /**
   * Get driver information.
   */
  private void writeDriverWebInformation(final HttpServletResponse response) throws IOException {

    LOG.log(Level.INFO, "HttpServerReefEventHandler writeDriverWebInformation invoked.");

    final PrintWriter writer = response.getWriter();

    writer.println("<h1>Driver Information:</h1>");

    writer.println(String.format("Driver Remote Identifier:[%s]",
        this.reefStateManager.getDriverEndpointIdentifier()));
    writer.write("<br/><br/>");

    writer.println(String.format("Services registered on Driver:"));
    writer.write("<br/><br/>");
    for (final AvroReefServiceInfo service : this.reefStateManager.getServicesInfo()) {
      writer.println(String.format("Service: [%s] , Information: [%s]", service.getServiceName(),
          service.getServiceInfo()));
      writer.write("<br/><br/>");
    }

    writer.println(String.format("Driver Start Time:[%s]", this.reefStateManager.getStartTime()));
  }

  /**
   * Write lines in ArrayList to the response writer.
   * @param response
   * @param lines
   * @param header
   * @throws IOException
   */
  private void writeLines(final HttpServletResponse response, final ArrayList<String> lines, final String header)
      throws IOException {
    LOG.log(Level.INFO, "HttpServerReefEventHandler writeLines is called");

    final PrintWriter writer = response.getWriter();

    writer.println("<h1>" + header + "</h1>");

    for (final String line : lines) {
      writer.println(line);
      writer.write("<br/>");
    }
    writer.write("<br/>");
  }
}
