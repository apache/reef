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

package org.apache.reef.bridge;

import org.apache.reef.annotations.audience.Private;

/**
 * List of handlers that can be registered by clients.
 */
@Private
public final class HandlerLabels {

  public static final String START = "start";

  public static final String STOP = "stop";

  public static final String ALLOCATED_EVAL = "allocated-evaluator";

  public static final String COMPLETE_EVAL = "complete-evaluator";

  public static final String FAILED_EVAL = "failed-evaluator";

  public static final String ACTIVE_CXT = "active-context";

  public static final String CLOSED_CXT = "closed-context";

  public static final String MESSAGE_CXT = "context-message";

  public static final String FAILED_CXT = "failed-context";

  public static final String RUNNING_TASK = "running-task";

  public static final String FAILED_TASK = "failed-task";

  public static final String COMPLETED_TASK = "completed-task";

  public static final String SUSPENDED_TASK = "suspended-task";

  public static final String TASK_MESSAGE = "task-message";

  public static final String CLIENT_MESSAGE = "client-message";

  public static final String CLIENT_CLOSE = "client-close";

  public static final String CLIENT_CLOSE_WITH_MESSAGE = "client-close-with-message";

  public static final String HANDLER_LABEL_SEPERATOR = ";";

  public static final String HANDLER_LABEL_DESCRIPTION = "Handler Event Labels: \n" +
      "> " + HandlerLabels.START + "\n" +
      "> " + HandlerLabels.STOP + "\n" +
      "> " + HandlerLabels.ALLOCATED_EVAL + "\n" +
      "> " + HandlerLabels.COMPLETE_EVAL + "\n" +
      "> " + HandlerLabels.FAILED_EVAL + "\n" +
      "> " + HandlerLabels.ACTIVE_CXT + "\n" +
      "> " + HandlerLabels.CLOSED_CXT + "\n" +
      "> " + HandlerLabels.MESSAGE_CXT + "\n" +
      "> " + HandlerLabels.FAILED_CXT + "\n" +
      "> " + HandlerLabels.RUNNING_TASK + "\n" +
      "> " + HandlerLabels.FAILED_TASK + "\n" +
      "> " + HandlerLabels.COMPLETED_TASK + "\n" +
      "> " + HandlerLabels.SUSPENDED_TASK + "\n" +
      "> " + HandlerLabels.TASK_MESSAGE + "\n" +
      "> " + HandlerLabels.CLIENT_MESSAGE + "\n" +
      "> " + HandlerLabels.CLIENT_CLOSE + "\n" +
      "> " + HandlerLabels.CLIENT_CLOSE_WITH_MESSAGE + "\n" +
      "Specify a list of handler event labels seperated by '" +
      HANDLER_LABEL_SEPERATOR + "'\n" +
      "e.g., \"" + HandlerLabels.START + HANDLER_LABEL_SEPERATOR + HandlerLabels.STOP +
      "\" registers for the stop and start handlers, but none other.";

  private HandlerLabels() {}
}
