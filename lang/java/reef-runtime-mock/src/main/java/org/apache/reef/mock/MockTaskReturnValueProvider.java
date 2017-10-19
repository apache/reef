/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */

package org.apache.reef.mock;

import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.mock.runtime.MockRunningTask;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Clients bind an implementation of this interface, which
 * will be used to create a mock return value for a mock
 * task execution. This return value will be returned by
 * the {@link CompletedTask#get()}} method.
 */
@DefaultImplementation(DefaultTaskReturnValueProvider.class)
public interface MockTaskReturnValueProvider {

  /**
   * Provide a valid return value for the {@link CompletedTask#get()} method.
   * @param task that is to be provided with a return value
   * @return {@link org.apache.reef.task.Task#call(byte[])} return value
   */
  byte[] getReturnValue(final MockRunningTask task);
}
