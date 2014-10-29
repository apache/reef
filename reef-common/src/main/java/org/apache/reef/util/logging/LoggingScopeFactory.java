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

package org.apache.reef.util.logging;

import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.Date;
import java.util.logging.Logger;

/**
 * Create Logging scope objects
 */
public class LoggingScopeFactory {

  private static final Logger LOG = Logger.getLogger(LoggingScopeFactory.class.getName());
  public static final String DRIVER_START = "Driver Start Handler";
  public static final String DRIVER_STOP = "Driver Stop Handler";
  public static final String BRIDGE_SETUP = "Bridge setup";
  public static final String EVALUATOR_REQUESTOR = "Evaluator requestor passed to C#";
  public static final String EVALUATOR_BRIDGE_SUBMIT = "Evaluator request submit cross bridge";
  public static final String EVALUATOR_SUBMIT = "Evaluator submit";
  public static final String EVALUATOR_LAUNCH = "Evaluator launch";
  public static final String EVALUATOR_ALLOCATED = "Evaluator allocated";
  public static final String EVALUATOR_COMPLETED = "Evaluator completed";
  public static final String EVALUATOR_FAILED = "Evaluator failed";
  public static final String ACTIVE_CONTEXT = "Active context created";
  public static final String TASK_RUNNING = "Task running";
  public static final String TASK_COMPLETE = "Task complete";
  public static final String TASK_MESSAGE = "Task message";
  public static final String CONTEXT_MESSAGE = "Context message";
  public static final String CONTEXT_CLOSE = "Context close";
  public static final String DRIVER_RESTART = "Driver restart";
  public static final String DRIVER_RESTART_COMPLETE = "Driver restart complete";
  public static final String DRIVER_RESTART_RUNNING_TASK = "Driver restart running task";
  public static final String DRIVER_RESTART_ACTIVE_CONTEXT = "Driver restart active context";
  public static final String TASK_SUSPEND = "Task suspend";
  public static final String DRIVER_SUBMIT = "Driver submit";
  public static final String REEF_SUBMIT = "Reef submit";
  public static final String LOCAL_JOB_SUBMIT = "Local job submit";
  public static final String HTTP_REQUEST = "Http request";
  public static final String HTTP_SERVER = "Http server";

  @Inject
  public LoggingScopeFactory() {
  }

  public LoggingScope scopeLogger(final String msg) {
    return new LoggingScope(LOG, msg);
  }

  public LoggingScope scopeLogger(final String msg, final Object params[]) {
    return new LoggingScope(LOG, msg, params);
  }

  public LoggingScope driverStart(final StartTime startTime) {
    return new LoggingScope(LOG, DRIVER_START + "startTime: " + startTime);
  }

  public LoggingScope driverStop(final long timeStamp) {
    return new LoggingScope(LOG, this.DRIVER_STOP + " :" + timeStamp);
  }

  public LoggingScope setupBridge() {
    return new LoggingScope(LOG, BRIDGE_SETUP);
  }

  public LoggingScope evaluatorRequestorPassToCs() {
    return new LoggingScope(LOG, EVALUATOR_REQUESTOR);
  }

  public LoggingScope evaluatorRequestSubmitToJavaDriver(int evaluatorsNumber) {
    return new LoggingScope(LOG, EVALUATOR_BRIDGE_SUBMIT + ":" + evaluatorsNumber);
  }

  public LoggingScope evaluatorSubmit(int evaluatorNumber) {
    return new LoggingScope(LOG, EVALUATOR_SUBMIT + ":" + evaluatorNumber);
  }

  public LoggingScope evaluatorAllocated(final String evaluatorId) {
    return new LoggingScope(LOG, EVALUATOR_ALLOCATED + " :" + evaluatorId);
  }

  public LoggingScope evaluatorLaunch(String evaluatorId) {
    return new LoggingScope(LOG, EVALUATOR_LAUNCH + " :" + evaluatorId);
  }

  public LoggingScope evaluatorCompleted(final String evaluatoerId) {
    return new LoggingScope(LOG, EVALUATOR_COMPLETED + " :" + evaluatoerId);
  }

  public LoggingScope evaluatorFailed(final String evaluatorId) {
    return new LoggingScope(LOG, this.EVALUATOR_FAILED + " :" + evaluatorId);
  }

  public LoggingScope activeContextReceived(final String contextId) {
    return new LoggingScope(LOG, ACTIVE_CONTEXT + " :" + contextId);
  }

  public LoggingScope closedContext(final String contextId) {
    return new LoggingScope(LOG, this.CONTEXT_CLOSE + " :" + contextId);
  }

  public LoggingScope taskRunning(final String taskId) {
    return new LoggingScope(LOG, TASK_RUNNING + " :" + taskId);
  }

  public LoggingScope taskCompleted(final String taskId) {
    return new LoggingScope(LOG, TASK_COMPLETE + " :" + taskId);
  }

  public LoggingScope taskSuspended(final String taskId) {
    return new LoggingScope(LOG, TASK_SUSPEND + " :" + taskId);
  }

  public LoggingScope taskMessageReceived(final String msg) {
    return new LoggingScope(LOG, TASK_MESSAGE + " :" + msg);
  }

  public LoggingScope contextMessageReceived(final String msg) {
    return new LoggingScope(LOG, CONTEXT_MESSAGE + " :" + msg);
  }

  public LoggingScope driverRestart(final StartTime startTime) {
    return new LoggingScope(LOG, DRIVER_RESTART + " :" + startTime);
  }

  public LoggingScope driverRestartCompleted(final long timeStamp) {
    return new LoggingScope(LOG, DRIVER_RESTART_COMPLETE + " :" + timeStamp);
  }

  public LoggingScope driverRestartRunningTask(final String taskId) {
    return new LoggingScope(LOG, DRIVER_RESTART_RUNNING_TASK + " :" + taskId);
  }

  public LoggingScope driverRestartActiveContextReceived(final String contextId) {
    return new LoggingScope(LOG, DRIVER_RESTART_ACTIVE_CONTEXT + " :" + contextId);
  }

//  public LoggingScope submitContextAndServiceAndTaskString(String context, String server, String task) {
//    return new LoggingScope(LOG, "Submit context {0} and server {1} and task {2} string", new Object[] { context, server, task });
//  }
//
//  public LoggingScope submitContextAndServiceString(String context, String server) {
//    return new LoggingScope(LOG, "Submit context {0} and server {1} string", new Object[] { context, server });
//  }
//
//  public LoggingScope submitContextAndTaskString(String context, String task) {
//    return new LoggingScope(LOG, "Submit context {0} and task {1} string", new Object[] { context, task });
//  }
//
//  public LoggingScope submitContextString(String context) {
//    return new LoggingScope(LOG, "Submit context string {0}.", new Object[] { context });
//  }

  public LoggingScope httpRequest(final String uri) {
    return new LoggingScope(LOG, this.HTTP_REQUEST + " :" + uri);
  }

  public LoggingScope httpServer() {
    return new LoggingScope(LOG, this.HTTP_SERVER);
  }

  public LoggingScope driverSubmit(Boolean submitDriver) {
    return new LoggingScope(LOG, DRIVER_SUBMIT + " :" + submitDriver);
  }

  public LoggingScope reefSubmit() {
    return new LoggingScope(LOG, this.REEF_SUBMIT);
  }

  public LoggingScope localJobSubmission() {
    return new LoggingScope(LOG, this.LOCAL_JOB_SUBMIT);
  }
}
