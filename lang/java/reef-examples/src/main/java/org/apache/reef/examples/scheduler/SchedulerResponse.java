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

/**
 * This class specifies the response from the Scheduler.
 * It includes the status code and message.
 */
final class SchedulerResponse {
  /**
   * 200 OK : The request succeeded normally.
   */
  private static final int SC_OK = 200;

  /**
   * 400 BAD REQUEST : The request is syntactically incorrect.
   */
  private static final int SC_BAD_REQUEST = 400;

  /**
   * 403 FORBIDDEN : Syntactically okay but refused to process.
   */
  private static final int SC_FORBIDDEN = 403;

  /**
   * 404 NOT FOUND :  The resource is not available.
   */
  private static final int SC_NOT_FOUND = 404;

  /**
   * Create a response with OK status
   */
  public static SchedulerResponse OK(final String message){
    return new SchedulerResponse (SC_OK, message);
  }

  /**
   * Create a response with BAD_REQUEST status
   */
  public static SchedulerResponse BAD_REQUEST(final String message){
    return new SchedulerResponse (SC_BAD_REQUEST, message);
  }

  /**
   * Create a response with FORBIDDEN status
   */
  public static SchedulerResponse FORBIDDEN(final String message){
    return new SchedulerResponse (SC_FORBIDDEN, message);
  }

  /**
   * Create a response with NOT FOUND status
   */
  public static SchedulerResponse NOT_FOUND(final String message){
    return new SchedulerResponse (SC_NOT_FOUND, message);
  }

  /**
   * Return {@code true} if the response is OK.
   */
  public boolean isOK(){
    return this.status == SC_OK;
  }

  /**
   * Status code of the request based on RFC 2068.
   */
  private int status;

  /**
   * Message to send.
   */
  private String message;

  /**
   * Constructor using status code and message.
   * @param status
   * @param message
   */
  private SchedulerResponse(final int status, final String message) {
    this.status = status;
    this.message = message;
  }

  /**
   * Return the status code of this response.
   */
  int getStatus() {
    return status;
  }

  /**
   * Return the message of this response.
   */
  String getMessage() {
    return message;
  }
}
