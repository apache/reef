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

import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.runtime.common.driver.client.JobStatusHandler;
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

  @Inject
  DriverStatusHTTPHandler() {

  }

  @Override
  public String getUriSpecification() {
    return uriSpecification;
  }

  @Override
  public void setUriSpecification(final String s) {
    this.uriSpecification = s;
  }

  /**
   * A hanging HTTP request with the status of the Driver.
   *
   * @param parsedHttpRequest
   * @param response
   * @throws IOException
   * @throws ServletException
   */
  @Override
  public void onHttpRequest(final ParsedHttpRequest parsedHttpRequest, final HttpServletResponse response)
      throws IOException, ServletException {
    try (final PrintWriter writer = response.getWriter()) {
      writer.write(waitAndGetMessage());
    }
  }

  @Override
  public void onNext(final ReefServiceProtos.JobStatusProto value) {
    LOG.log(Level.INFO, "Received status: " + value.getState().name());
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
        } catch (InterruptedException e) {
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
   * @param status
   * @return
   */
  static String getMessageForStatus(final ReefServiceProtos.JobStatusProto status) {
    return status.getState().name();
  }
}
