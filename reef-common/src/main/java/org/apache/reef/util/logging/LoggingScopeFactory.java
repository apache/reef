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

package org.apache.reef.util.logging;

import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.logging.Level;
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

  /**
   * Log level. Client can set it through LogLevelName named parameter
   */
  private final Level logLevel;

  /**
   * User can inject a LoggingScopeFactory with injected log level as a string
   */
  @Inject
  private LoggingScopeFactory(@Parameter(LogLevelName.class) final String logLevelName) {
    this.logLevel = Level.parse(logLevelName);
  }

  /**
   * Get a new instance of LoggingScope with msg through new
   * @param msg
   * @return
   */
  public LoggingScope getNewLoggingScope(final String msg) {
    return new LoggingScopeImpl(LOG, logLevel, msg);
  }

  /**
   * Get a new instance of LoggingScope with msg and params through new
   * @param msg
   * @param params
   * @return
   */
  public LoggingScope getNewLoggingScope(final String msg, final Object params[]) {
    return new LoggingScopeImpl(LOG, logLevel, msg, params);
  }

  /**
   * The method is to measure the time used to start the driver. It can be inserted to the code between start driver till it is started
   * @param startTime
   * @return
   */
  public LoggingScope driverStart(final StartTime startTime) {
    return new LoggingScopeImpl(LOG, logLevel, DRIVER_START + " :" + startTime);
  }

  /**
   * The method is to measure the time used to stop the driver. It can be inserted to the code between start driver stop till it is stopped
   * @param timeStamp
   * @return
   */
  public LoggingScope driverStop(final long timeStamp) {
    return new LoggingScopeImpl(LOG, logLevel, this.DRIVER_STOP + " :" + timeStamp);
  }

  /**
   * The method is to measure the time used to set up Java CRL bridge. It can be inserted to the code between beginning of bridge set up and the end of it
   * @return
   */
  public LoggingScope setupBridge() {
    return new LoggingScopeImpl(LOG, logLevel, BRIDGE_SETUP);
  }

  /**
   * The method is to measure the time used to pass EvaluatorRequestor from Java to .Net. It can be inserted to the code between beginning to send EvaluatorRequestor to CLR until it is returned.
   * @return
   */
  public LoggingScope evaluatorRequestorPassToCs() {
    return new LoggingScopeImpl(LOG, logLevel, EVALUATOR_REQUESTOR);
  }

  /**
   * The method is to measure the time used to submit Evaluator request from CLR to Java driver. It can be inserted to evaluator submit() method.
   * @param evaluatorsNumber
   * @return
   */
  public LoggingScope evaluatorRequestSubmitToJavaDriver(final int evaluatorsNumber) {
    return new LoggingScopeImpl(LOG, logLevel, EVALUATOR_BRIDGE_SUBMIT + ":" + evaluatorsNumber);
  }

  /**
   * The method is to measure the time used to submit a Evaluator request at java side
   * @param evaluatorNumber
   * @return
   */
  public LoggingScope evaluatorSubmit(final int evaluatorNumber) {
    return new LoggingScopeImpl(LOG, logLevel, EVALUATOR_SUBMIT + ":" + evaluatorNumber);
  }

  /**
   * This is to measure the time on evaluatorAllocated handler
   * @param evaluatorId
   * @return
   */
  public LoggingScope evaluatorAllocated(final String evaluatorId) {
    return new LoggingScopeImpl(LOG, logLevel, EVALUATOR_ALLOCATED + " :" + evaluatorId);
  }

  /**
   * This is to measure the time to launch an evaluator
   * @param evaluatorId
   * @return
   */
  public LoggingScope evaluatorLaunch(final String evaluatorId) {
    return new LoggingScopeImpl(LOG, logLevel, EVALUATOR_LAUNCH + " :" + evaluatorId);
  }

  /**
   * This is to measure the time in calling evaluatorCompleted handler
   * @param evaluatorId
   * @return
   */
  public LoggingScope evaluatorCompleted(final String evaluatorId) {
    return new LoggingScopeImpl(LOG, logLevel, EVALUATOR_COMPLETED + " :" + evaluatorId);
  }

  /**
   * This is to measure the time in calling evaluatorFailed handler
   * @param evaluatorId
   * @return
   */
  public LoggingScope evaluatorFailed(final String evaluatorId) {
    return new LoggingScopeImpl(LOG, logLevel, this.EVALUATOR_FAILED + " :" + evaluatorId);
  }

  /**
   * This is to measure the time in calling activeContext handler
   * @param contextId
   * @return
   */
  public LoggingScope activeContextReceived(final String contextId) {
    return new LoggingScopeImpl(LOG, logLevel, ACTIVE_CONTEXT + " :" + contextId);
  }

  /**
   * This is to measure the time in calling closedContext handler
   * @param contextId
   * @return
   */
  public LoggingScope closedContext(final String contextId) {
    return new LoggingScopeImpl(LOG, logLevel, this.CONTEXT_CLOSE + " :" + contextId);
  }

  /**
   * This is to measure the time in calling runningTaskHandler
   * @param taskId
   * @return
   */
  public LoggingScope taskRunning(final String taskId) {
    return new LoggingScopeImpl(LOG, logLevel, TASK_RUNNING + " :" + taskId);
  }

  /**
   * This is to measure the time in calling taskCompletedHandler
   * @param taskId
   * @return
   */
  public LoggingScope taskCompleted(final String taskId) {
    return new LoggingScopeImpl(LOG, logLevel, TASK_COMPLETE + " :" + taskId);
  }

  /**
   * This is to measure the time in calling taskSuspendedHandler
   * @param taskId
   * @return
   */
  public LoggingScope taskSuspended(final String taskId) {
    return new LoggingScopeImpl(LOG, logLevel, TASK_SUSPEND + " :" + taskId);
  }

  /**
   * This is to measure the time in calling taskMessageReceivedHandler
   * @param msg
   * @return
   */
  public LoggingScope taskMessageReceived(final String msg) {
    return new LoggingScopeImpl(LOG, logLevel, TASK_MESSAGE + " :" + msg);
  }

  /**
   * This is to measure the time in calling contextMessageReceivedHandler
   * @param msg
   * @return
   */
  public LoggingScope contextMessageReceived(final String msg) {
    return new LoggingScopeImpl(LOG, logLevel, CONTEXT_MESSAGE + " :" + msg);
  }

  /**
   * This is to measure the time in calling driverRestartHandler
   * @param startTime
   * @return
   */
  public LoggingScope driverRestart(final StartTime startTime) {
    return new LoggingScopeImpl(LOG, logLevel, DRIVER_RESTART + " :" + startTime);
  }

  /**
   * This is to measure the time in calling driverRestartCompletedHandler
   * @param timeStamp
   * @return
   */
  public LoggingScope driverRestartCompleted(final long timeStamp) {
    return new LoggingScopeImpl(LOG, logLevel, DRIVER_RESTART_COMPLETE + " :" + timeStamp);
  }

  /**
   * This is to measure the time in calling driverRestartRunningTaskHandler
   * @param taskId
   * @return
   */
  public LoggingScope driverRestartRunningTask(final String taskId) {
    return new LoggingScopeImpl(LOG, logLevel, DRIVER_RESTART_RUNNING_TASK + " :" + taskId);
  }

  /**
   * This is to measure the time in calling driverRestartActiveContextReceivedHandler
   * @param contextId
   * @return
   */
  public LoggingScope driverRestartActiveContextReceived(final String contextId) {
    return new LoggingScopeImpl(LOG, logLevel, DRIVER_RESTART_ACTIVE_CONTEXT + " :" + contextId);
  }

  /**
   * This is to measure the time in handling a http request
   * @param uri
   * @return
   */
  public LoggingScope httpRequest(final String uri) {
    return new LoggingScopeImpl(LOG, logLevel, this.HTTP_REQUEST + " :" + uri);
  }

  /**
   * This is to measure the time used to create HttpServer
   * @return
   */
  public LoggingScope httpServer() {
    return new LoggingScopeImpl(LOG, logLevel, this.HTTP_SERVER);
  }

  /**
   * This is to measure the time to submit a driver
   * @param submitDriver
   * @return
   */
  public LoggingScope driverSubmit(final Boolean submitDriver) {
    return new LoggingScopeImpl(LOG, logLevel, DRIVER_SUBMIT + " :" + submitDriver);
  }

  /**
   * This is to measure the time to call Reef.Submit
   * @return
   */
  public LoggingScope reefSubmit() {
    return new LoggingScopeImpl(LOG, logLevel, this.REEF_SUBMIT);
  }

  /**
   * This is to measure the time for a job submission
   * @return
   */
  public LoggingScope localJobSubmission() {
    return new LoggingScopeImpl(LOG, logLevel, this.LOCAL_JOB_SUBMIT);
  }
}
