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
package org.apache.reef.bridge.client;

import org.apache.reef.bridge.client.Parameters.HTTPStatusAlarmInterval;
import org.apache.reef.bridge.client.Parameters.HTTPStatusNumberOfRetries;
import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.runtime.common.driver.client.JobStatusHandler;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.Clock;
import org.apache.reef.wake.time.event.Alarm;
import org.apache.reef.webserver.HttpHandler;
import org.apache.reef.webserver.ParsedHttpRequest;

import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

final class DriverStatusHTTPHandler implements HttpHandler, JobStatusHandler {

  private static final Logger LOG = Logger.getLogger(DriverStatusHTTPHandler.class.getName());

  /**
   * The URI under which this handler answers.
   */
  private String uriSpecification = "driverstatus";

  /**
   * A queue of messages to be sent to the client.
   */
  private final Queue<ReefServiceProtos.JobStatusProto> statusMessagesToSend = new LinkedList<>();

  /**
   * The last status received by this object in its role as JobStatusHandler.
   */
  private ReefServiceProtos.JobStatusProto lastStatus = null;

  /**
   * The clock is used to schedule a check whether the handler has been called.
   */
  private final Clock clock;

  /**
   * The maximum number of times the AlarmHandler will be scheduled.
   */
  private final int maxNumberOfRetries;

  /**
   * The interval between alarms.
   */
  private final int alarmInterval;

  /**
   * The current retry.
   */
  private int retry = 0;

  /**
   * The alarm handler to keep the Clock alive until the status has been requested once.
   */
  private final EventHandler<Alarm> alarmHandler = new EventHandler<Alarm>() {
    @Override
    public void onNext(final Alarm value) {
      scheduleAlarm();
    }
  };

  /**
   * Whether or not this handler was called at least once via HTTP.
   */
  private boolean wasCalledViaHTTP = false;

  @Inject
  DriverStatusHTTPHandler(final Clock clock,
                          @Parameter(HTTPStatusNumberOfRetries.class) final int maxNumberOfRetries,
                          @Parameter(HTTPStatusAlarmInterval.class) final int alarmInterval) {
    this.clock = clock;
    this.maxNumberOfRetries = maxNumberOfRetries;
    this.alarmInterval = alarmInterval;
    scheduleAlarm();
  }

  @Override
  public String getUriSpecification() {
    return uriSpecification;
  }

  @Override
  public void setUriSpecification(final String newUriSpecification) {
    this.uriSpecification = newUriSpecification;
  }

  @Override
  public void onHttpRequest(final ParsedHttpRequest parsedHttpRequest, final HttpServletResponse response)
      throws IOException, ServletException {
    try (PrintWriter writer = response.getWriter()) {
      writer.write(waitAndGetMessage());
      this.wasCalledViaHTTP = true;
    }
  }

  @Override
  public void onNext(final ReefServiceProtos.JobStatusProto value) {
    LOG.log(Level.INFO, "Received status: {0}", value.getState().name());
    // Record the status received and notify the thread to send an answer.
    synchronized (this) {
      this.statusMessagesToSend.add(value);
      this.lastStatus = value;
      this.notifyAll();
    }
  }

  @Override
  public ReefServiceProtos.JobStatusProto getLastStatus() {
    return this.lastStatus;
  }

  @Override
  public String toString() {
    return "DriverStatusHTTPHandler{uriSpec=" + getUriSpecification() + "}";
  }

  /**
   * Waits for a status message to be available and returns it.
   *
   * @return the first available status message.
   */
  String waitAndGetMessage() {
    synchronized (this) {
      // Wait for a message to send.
      while (this.statusMessagesToSend.isEmpty()) {
        try {
          this.wait();
        } catch (final InterruptedException e) {
          LOG.log(Level.FINE, "Interrupted. Ignoring.");
        }
      }

      // Send the message
      return getMessageForStatus(this.statusMessagesToSend.poll());
    }
  }

  /**
   * Generates a string to be sent to the client based on a
   * {@link org.apache.reef.proto.ReefServiceProtos.JobStatusProto}.
   *
   * @param status the status to be converted to String.
   * @return the string to be sent back to the HTTP client.
   */
  static String getMessageForStatus(final ReefServiceProtos.JobStatusProto status) {
    return status.getState().name();
  }

  /**
   * Schedules an alarm, if needed.
   * <p>
   * The alarm will prevent the Clock from going idle. This gives the .NET Client time to make a call to this HTTP
   * handler.
   */
  private void scheduleAlarm() {
    if (wasCalledViaHTTP || retry >= maxNumberOfRetries) {
      // No alarm necessary anymore.
      LOG.log(Level.INFO,
          "Not scheduling additional alarms after {0} out of max {1} retries. The HTTP handles was called: ",
          new Object[] {retry, maxNumberOfRetries, wasCalledViaHTTP});
      return;
    }

    // Scheduling an alarm will prevent the clock from going idle.
    ++retry;
    clock.scheduleAlarm(alarmInterval, alarmHandler);
  }
}
