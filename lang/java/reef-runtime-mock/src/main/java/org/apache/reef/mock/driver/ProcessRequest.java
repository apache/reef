/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.reef.mock.driver;

import org.apache.reef.annotations.Unstable;

/**
 * A ProcessRequest refers to an outstanding event that is
 * waiting to be processed by the REEF mock runtime. Clients
 * are responsible for deciding how a ProcessRequest should be
 * handled, by either:
 * 1. successfully processing the request
 * 2. unsucessfully processing the request
 * 3. dropping the processing request (i.e., loosing it)
 * These decisions are conveyed through the {MockRuntime} API.
 */
@Unstable
public interface ProcessRequest extends AutoCompletable {
  /**
   * process request type.
   */
  enum Type {
    ALLOCATE_EVALUATOR,
    CLOSE_EVALUATOR,
    CREATE_CONTEXT,
    CLOSE_CONTEXT,
    CREATE_TASK,
    SUSPEND_TASK,
    CLOSE_TASK,
    COMPLETE_TASK,
    CREATE_CONTEXT_AND_TASK,
    SEND_MESSAGE_DRIVER_TO_TASK,
    SEND_MESSAGE_DRIVER_TO_CONTEXT
  }

  Type getType();
}
