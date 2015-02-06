/**
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
package org.apache.reef.examples.scheduler;

import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.webserver.HttpHandler;
import org.apache.reef.webserver.ParsedHttpRequest;

import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Receive HttpRequest so that it can handle the command list
 */
final class SchedulerHttpHandler implements HttpHandler {
  final InjectionFuture<SchedulerDriver> schedulerDriver;

  private String uriSpecification = "reef-example-scheduler";

  @Inject
  public SchedulerHttpHandler(final InjectionFuture<SchedulerDriver> schedulerDriver) {
    this.schedulerDriver = schedulerDriver;
  }

  @Override
  public String getUriSpecification() {
    return uriSpecification;
  }

  @Override
  public void setUriSpecification(String s) {
    uriSpecification = s;
  }

  /**
   * HttpRequest handler. You must specify UriSpecification and REST API version.
   * The request url is http://{address}:{port}/reef-example-scheduler/v1
   *
   * APIs
   *   /list                to get the status list for all tasks
   *   /status?id={id}      to query the status of such a task, given id
   *   /submit?cmd={cmd}    to submit a Task, which returns its id
   *   /cancel?id={id}      to cancel the task's execution
   *   /num-eval?num={num}  to set the maximum number of evaluators
   *   /clear               to clear the waiting queue
   */
  @Override
  public void onHttpRequest(ParsedHttpRequest request, HttpServletResponse response)
    throws IOException, ServletException {
    final String target = request.getTargetEntity().toLowerCase();
    final Map<String, List<String>> queryMap = request.getQueryMap();

    final SchedulerResponse result;
    switch (target) {
      case "list":
        result = schedulerDriver.get().getList();
        break;
      case "clear":
        result = schedulerDriver.get().clearList();
        break;
      case "status":
        result = schedulerDriver.get().getTaskStatus(queryMap.get("id"));
        break;
      case "submit":
        result = schedulerDriver.get().submitCommands(queryMap.get("cmd"));
        break;
      case "cancel":
        result = schedulerDriver.get().cancelTask(queryMap.get("id"));
        break;
      case "max-eval":
        result = schedulerDriver.get().setMaxEvaluators(queryMap.get("num"));
        break;
      default:
        result = SchedulerResponse.NOT_FOUND("Unsupported operation");
    }

    // Send response to the http client
    final int status = result.getStatus();
    final String message= result.getMessage();

    if (result.isOK()) {
      response.getOutputStream().println(message);
    } else {
      response.sendError(status, message);
    }
  }
}
