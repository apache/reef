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
package org.apache.reef.runtime.common.client.defaults;

import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runtime.common.UserCredentials;

import javax.inject.Inject;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;

/**
 * A holder object for REEF user credentials.
 */
@Private
@ClientSide
@DriverSide
public final class DefaultUserCredentials implements UserCredentials {

  @Inject
  private DefaultUserCredentials() { }

  /**
   * Copy credentials from another existing user.
   * This method can be called only once per instance.
   * This default implementation should never be called.
   * @param name Name of the new user.
   * @param other Credentials of another user.
   * @throws RuntimeException always throws.
   */
  @Override
  public void set(final String name, final UserCredentials other) throws IOException {
    throw new RuntimeException("Not implemented! Attempt to set user " + name + " from: " + other);
  }

  /**
   * Check if the user credentials had been set. Always returns false.
   * @return always false.
   */
  @Override
  public boolean isSet() {
    return false;
  }

  /**
   * Execute the privileged action as a given user.
   * This implementation always executes the action outside the user context, simply by calling action.run().
   * @param action an action to run.
   * @param <T> action return type.
   * @return result of an action.
   * @throws Exception whatever the action can throw.
   */
  @Override
  public <T> T doAs(final PrivilegedExceptionAction<T> action) throws Exception {
    return action.run();
  }
}
