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
package org.apache.reef.runtime.common;

import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Public;
import org.apache.reef.runtime.common.client.defaults.DefaultUserCredentials;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

/**
 * A holder object for REEF user credentials.
 * Implementations of this interface are used e.g. for Unmanaged AM applications on YARN.
 */
@Public
@ClientSide
@DriverSide
@DefaultImplementation(DefaultUserCredentials.class)
public interface UserCredentials {

  /**
   * Copy credentials from another existing user.
   * This method can be called only once per instance.
   * @param name name of the new user.
   * @param other Credentials of another user.
   * @throws IOException if unable to copy.
   */
  void set(String name, UserCredentials other) throws IOException;

  /**
   * Check if the user credentials had been set.
   * @return true if set() method had been called successfully before, false otherwise.
   */
  boolean isSet();

  /**
   * Execute the privileged action as a given user.
   * If user credentials are not set, execute the action outside the user context.
   * @param action an action to run.
   * @param <T> action return type.
   * @return result of an action.
   * @throws Exception whatever the action can throw.
   */
  <T> T doAs(PrivilegedExceptionAction<T> action) throws Exception;
}
