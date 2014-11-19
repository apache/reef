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
public class SchedulerResponse {
  /**
   * 200 OK : The request succeeded normally.
   */
  public static final int SC_OK = 200;

  /**
   * 400 BAD REQUEST : The request is syntactically incorrect.
   */
  public static final int SC_BAD_REQUEST = 400;

  /**
   * 403 FORBIDDEN : Syntactically okay but refused to process.
   */
  public static final int SC_FORBIDDEN = 403;

  /**
   * 404 NOT FOUND :  The resource is not available.
   */
  public static final int SC_NOT_FOUND = 404;

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
  public SchedulerResponse(final int status, final String message) {
    this.status = status;
    this.message = message;
  }

  /**
   * Return the status code of this response.
   */
  public int getStatus() {
    return status;
  }

  /**
   * Return the message of this response.
   */
  public String getMessage() {
    return message;
  }
}
