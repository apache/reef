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
package org.apache.reef.examples.scheduler.driver.http;

import org.apache.reef.examples.scheduler.driver.SchedulerDriver;
import org.apache.reef.examples.scheduler.driver.exceptions.NotFoundException;
import org.apache.reef.examples.scheduler.driver.exceptions.UnsuccessfulException;
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
 * Receive HttpRequest so that it can handle the command list.
 */
public final class SchedulerHttpHandler implements HttpHandler {
  private final InjectionFuture<SchedulerDriver> schedulerDriver;

  private String uriSpecification = "reef-example-scheduler";

  @Inject
  private SchedulerHttpHandler(final InjectionFuture<SchedulerDriver> schedulerDriver) {
    this.schedulerDriver = schedulerDriver;
  }

  @Override
  public String getUriSpecification() {
    return uriSpecification;
  }

  @Override
  public void setUriSpecification(final String s) {
    uriSpecification = s;
  }

  /**
   * HttpRequest handler. You must specify UriSpecification and API version.
   * The request url is http://{address}:{port}/scheduler/v1
   *
   * APIs
   *   /list                to get the status list for all tasks
   *   /status?id={id}      to query the status of such a task, given id
   *   /submit?cmd={cmd}    to submit a Task, which returns its id
   *   /cancel?id={id}      to cancel the task's execution
   *   /max-eval?num={num}  to set the maximum number of evaluators
   *   /clear               to clear the waiting queue
   */
  @Override
  public void onHttpRequest(final ParsedHttpRequest request, final HttpServletResponse response)
      throws IOException, ServletException {
    final String target = request.getTargetEntity().toLowerCase();
    final Map<String, List<String>> queryMap = request.getQueryMap();

    final SchedulerResponse result;
    switch (target) {
    case "list":
      result = onList();
      break;
    case "clear":
      result = onClear();
      break;
    case "status":
      result = onStatus(queryMap);
      break;
    case "submit":
      result = onSubmit(queryMap);
      break;
    case "cancel":
      result = onCancel(queryMap);
      break;
    case "max-eval":
      result = onMaxEval(queryMap);
      break;
    default:
      result = SchedulerResponse.notFound("Unsupported operation");
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

  private SchedulerResponse onList() {
    final Map<String, List<Integer>> listMap = schedulerDriver.get().getList();

    final StringBuilder sb = new StringBuilder();
    for (final Map.Entry<String, List<Integer>> entry : listMap.entrySet()) {
      sb.append("\n").append(entry.getKey()).append(" :");
      for (final int taskId : entry.getValue()) {
        sb.append(" ").append(taskId);
      }
    }
    return SchedulerResponse.ok(sb.toString());
  }

  private SchedulerResponse onClear() {
    final int count = schedulerDriver.get().clearList();
    return SchedulerResponse.ok(count + " tasks removed.");
  }

  private SchedulerResponse onStatus(final Map<String, List<String>> queryMap) {
    final List<String> args = queryMap.get("id");
    if (args.size() != 1) {
      return SchedulerResponse.badRequest("Usage : only one ID at a time");
    }

    try {

      final int taskId = Integer.valueOf(args.get(0));
      return SchedulerResponse.ok(schedulerDriver.get().getTaskStatus(taskId));

    } catch (final NotFoundException e) {
      return SchedulerResponse.notFound(e.getMessage());
    } catch (final NumberFormatException e) {
      return SchedulerResponse.badRequest("Usage : ID must be an integer");
    }
  }

  private SchedulerResponse onSubmit(final Map<String, List<String>> queryMap) {
    final List<String> args = queryMap.get("cmd");
    if (args.size() != 1) {
      return SchedulerResponse.badRequest("Usage : only one command at a time");
    }

    return SchedulerResponse.ok("Task ID : " + schedulerDriver.get().submitCommand(args.get(0)));
  }

  private SchedulerResponse onCancel(final Map<String, List<String>> queryMap) {
    final List<String> args = queryMap.get("id");
    if (args.size() != 1) {
      return SchedulerResponse.badRequest("Usage : only one ID at a time");
    }

    try {

      final int taskId = Integer.valueOf(args.get(0));
      final int canceledId = schedulerDriver.get().cancelTask(taskId);
      return SchedulerResponse.ok("Canceled " + canceledId);

    } catch (final NotFoundException e) {
      return SchedulerResponse.notFound(e.getMessage());
    } catch (final UnsuccessfulException e) {
      return SchedulerResponse.forbidden(e.getMessage());
    } catch (final NumberFormatException e) {
      return SchedulerResponse.badRequest("Usage : ID must be an integer");
    }
  }

  private SchedulerResponse onMaxEval(final Map<String, List<String>> queryMap) {
    final List<String> args = queryMap.get("num");
    if (args.size() != 1) {
      return SchedulerResponse.badRequest("Usage : Only one value can be used");
    }

    try {

      final int targetNum = Integer.valueOf(args.get(0));
      final int maxEval = schedulerDriver.get().setMaxEvaluators(targetNum);
      return SchedulerResponse.ok("You can use up to " + maxEval + " evaluators.");

    } catch (final UnsuccessfulException e) {
      return SchedulerResponse.forbidden(e.getMessage());
    } catch (final NumberFormatException e) {
      return SchedulerResponse.badRequest("Usage : num must be an integer");
    }
  }
}
