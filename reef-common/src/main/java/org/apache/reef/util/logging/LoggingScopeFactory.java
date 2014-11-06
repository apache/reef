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

import org.apache.reef.runtime.common.launch.JavaLaunchCommandBuilder;
import org.apache.reef.tang.ConfigurationBuilder;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.time.event.StartTime;
import sun.rmi.runtime.Log;

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
    return new ReefLoggingScope(LOG, msg);
  }

  public LoggingScope scopeLogger(final String msg, final Object params[]) {
    return new ReefLoggingScope(LOG, msg, params);
  }

  public LoggingScope getLoggingScope(String msg) throws InjectionException {
    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindImplementation(LoggingScope.class, ReefLoggingScope.class);
    cb.bindNamedParameter(ReefLoggingScope.NamedString.class, msg);
    Injector i = Tang.Factory.getTang().newInjector(cb.build());
    i.bindVolatileInstance(Logger.class, LOG);
    return i.getInstance(LoggingScope.class);
  }

  /**
   * The method is to measure the time used to start the driver. It can be inserted to the code between start driver till it is started
   * @param startTime
   * @return
   */
  public LoggingScope driverStart(final StartTime startTime) {
    return new ReefLoggingScope(LOG, DRIVER_START + "startTime: " + startTime);
  }

  /**
   * The method is to measure the time used to stop the driver. It can be inserted to the code between start driver stop till it is stopped
   * @param timeStamp
   * @return
   */
  public LoggingScope driverStop(final long timeStamp) {
    return new ReefLoggingScope(LOG, this.DRIVER_STOP + " :" + timeStamp);
  }

  /**
   * The method is to measure the time used to set up Java CRL bridge. It can be inserted to the code between beginning of bridge set up and the end of it
   * @return
   */
  public LoggingScope setupBridge() {
    return new ReefLoggingScope(LOG, BRIDGE_SETUP);
  }

  /**
   * The method is to measure the time used to pass EvaluatorRequestor from Java to .Net. It can be inserted to the code between beginning to send EvaluatorRequestor to CLR until it is returned.
   * @return
   */
  public LoggingScope evaluatorRequestorPassToCs() {
    return new ReefLoggingScope(LOG, EVALUATOR_REQUESTOR);
  }

  /**
   * The method is to measure the time used to submit Evaluator request from CLR to Java driver. It can be inserted to evaluator submit() method.
   * @param evaluatorsNumber
   * @return
   */
  public LoggingScope evaluatorRequestSubmitToJavaDriver(int evaluatorsNumber) {
    return new ReefLoggingScope(LOG, EVALUATOR_BRIDGE_SUBMIT + ":" + evaluatorsNumber);
  }

  /**
   * The method is to measure the time used to submit a Evaluator request at java side
   * @param evaluatorNumber
   * @return
   */
  public LoggingScope evaluatorSubmit(int evaluatorNumber) {
    return new ReefLoggingScope(LOG, EVALUATOR_SUBMIT + ":" + evaluatorNumber);
  }

  /**
   * This is to measure the time on evaluatorAllocated handler
   * @param evaluatorId
   * @return
   */
  public LoggingScope evaluatorAllocated(final String evaluatorId) {
    return new ReefLoggingScope(LOG, EVALUATOR_ALLOCATED + " :" + evaluatorId);
  }

  /**
   * This is to measure the time to launch an evaluator
   * @param evaluatorId
   * @return
   */
  public LoggingScope evaluatorLaunch(String evaluatorId) {
    return new ReefLoggingScope(LOG, EVALUATOR_LAUNCH + " :" + evaluatorId);
  }

  /**
   * This is to measure the time in calling evaluatorCompleted handler
   * @param evaluatoerId
   * @return
   */
  public LoggingScope evaluatorCompleted(final String evaluatoerId) {
    return new ReefLoggingScope(LOG, EVALUATOR_COMPLETED + " :" + evaluatoerId);
  }

  /**
   * This is to measure the time in calling evaluatorFailed handler
   * @param evaluatorId
   * @return
   */
  public LoggingScope evaluatorFailed(final String evaluatorId) {
    return new ReefLoggingScope(LOG, this.EVALUATOR_FAILED + " :" + evaluatorId);
  }

  /**
   * This is to measure the time in calling activeContext handler
   * @param contextId
   * @return
   */
  public LoggingScope activeContextReceived(final String contextId) {
    return new ReefLoggingScope(LOG, ACTIVE_CONTEXT + " :" + contextId);
  }

  /**
   * THis is to measure the time in calling closedContext handler
   * @param contextId
   * @return
   */
  public LoggingScope closedContext(final String contextId) {
    return new ReefLoggingScope(LOG, this.CONTEXT_CLOSE + " :" + contextId);
  }

  /**
   * This is to measure the time in calling runningTaskHandler
   * @param taskId
   * @return
   */
  public LoggingScope taskRunning(final String taskId) {
    return new ReefLoggingScope(LOG, TASK_RUNNING + " :" + taskId);
  }

  /**
   * This is to measure the time in calling taskCompletedHandler
   * @param taskId
   * @return
   */
  public LoggingScope taskCompleted(final String taskId) {
    return new ReefLoggingScope(LOG, TASK_COMPLETE + " :" + taskId);
  }

  /**
   * This is to measure the time in calling taskSuspendedHandler
   * @param taskId
   * @return
   */
  public LoggingScope taskSuspended(final String taskId) {
    return new ReefLoggingScope(LOG, TASK_SUSPEND + " :" + taskId);
  }

  /**
   * This is to measure the time in calling taskMessageReceivedHandler
   * @param msg
   * @return
   */
  public LoggingScope taskMessageReceived(final String msg) {
    return new ReefLoggingScope(LOG, TASK_MESSAGE + " :" + msg);
  }

  /**
   * This is to measure the time in calling contextMessageReceivedHandler
   * @param msg
   * @return
   */
  public LoggingScope contextMessageReceived(final String msg) {
    return new ReefLoggingScope(LOG, CONTEXT_MESSAGE + " :" + msg);
  }

  /**
   * This is to measure the time in calling driverRestartHandler
   * @param startTime
   * @return
   */
  public LoggingScope driverRestart(final StartTime startTime) {
    return new ReefLoggingScope(LOG, DRIVER_RESTART + " :" + startTime);
  }

  /**
   * This is to measure the time in calling driverRestartCompletedHandler
   * @param timeStamp
   * @return
   */
  public LoggingScope driverRestartCompleted(final long timeStamp) {
    return new ReefLoggingScope(LOG, DRIVER_RESTART_COMPLETE + " :" + timeStamp);
  }

  /**
   * This is to measure the time in calling driverRestartRunningTaskHandler
   * @param taskId
   * @return
   */
  public LoggingScope driverRestartRunningTask(final String taskId) {
    return new ReefLoggingScope(LOG, DRIVER_RESTART_RUNNING_TASK + " :" + taskId);
  }

  /**
   * This is to measure the time in calling driverRestartActiveContextReceivedHandler
   * @param contextId
   * @return
   */
  public LoggingScope driverRestartActiveContextReceived(final String contextId) {
    return new ReefLoggingScope(LOG, DRIVER_RESTART_ACTIVE_CONTEXT + " :" + contextId);
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

  /**
   * This is to measure the time in handling a http request
   * @param uri
   * @return
   */
  public LoggingScope httpRequest(final String uri) {
    return new ReefLoggingScope(LOG, this.HTTP_REQUEST + " :" + uri);
  }

  /**
   * This is to measure the time used to create HttpServer
   * @return
   */
  public LoggingScope httpServer() {
    return new ReefLoggingScope(LOG, this.HTTP_SERVER);
  }

  /**
   * This is to measure the time to submit a driver
   * @param submitDriver
   * @return
   */
  public LoggingScope driverSubmit(Boolean submitDriver) {
    return new ReefLoggingScope(LOG, DRIVER_SUBMIT + " :" + submitDriver);
  }

  /**
   * This is to measure the time to call Reef.Submit
   * @return
   */
  public LoggingScope reefSubmit() {
    return new ReefLoggingScope(LOG, this.REEF_SUBMIT);
  }

  /**
   * This is to measure the time for a job submission
   * @return
   */
  public LoggingScope localJobSubmission() {
    return new ReefLoggingScope(LOG, this.LOCAL_JOB_SUBMIT);
  }
}
