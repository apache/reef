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
package org.apache.reef.client;

import org.apache.reef.util.Optional;

/**
 * The status of a reef job spawned using the DriverLauncher class.
 */
public final class LauncherStatus {

  public static final LauncherStatus INIT = new LauncherStatus(State.INIT);
  public static final LauncherStatus SUBMITTED = new LauncherStatus(State.SUBMITTED);
  public static final LauncherStatus RUNNING = new LauncherStatus(State.RUNNING);
  public static final LauncherStatus COMPLETED = new LauncherStatus(State.COMPLETED);
  public static final LauncherStatus FORCE_CLOSED = new LauncherStatus(State.FORCE_CLOSED);
  public static final LauncherStatus FAILED = new LauncherStatus(State.FAILED);

  private final State state;
  private final Optional<Throwable> error;

  private LauncherStatus(final State state) {
    this(state, null);
  }

  private LauncherStatus(final State state, final Throwable ex) {
    this.state = state;
    this.error = Optional.ofNullable(ex);
  }

  public static LauncherStatus failed(final Throwable ex) {
    return new LauncherStatus(State.FAILED, ex);
  }

  public static LauncherStatus failed(final Optional<Throwable> ex) {
    return new LauncherStatus(State.FAILED, ex.orElse(null));
  }

  public Optional<Throwable> getError() {
    return this.error;
  }

  /**
   * Compare the <b>State</b> of two LauncherStatus objects.
   * Note that it does NOT compare the exceptions - just the states.
   *
   * @return True if both LauncherStatus objects are in the same state.
   */
  @Override
  public boolean equals(final Object other) {
    return this == other ||
        other instanceof LauncherStatus && ((LauncherStatus) other).state == this.state;
  }

  @Override
  public int hashCode() {
    return state.hashCode();
  }

  /**
   * Has the job completed?
   *
   * @return True if the job has been completed, false otherwise.
   */
  public boolean isDone() {
    switch (this.state) {
    case FAILED:
    case COMPLETED:
    case FORCE_CLOSED:
      return true;
    default:
      return false;
    }
  }

  /**
   * Has the job completed successfully?
   *
   * @return True if the job has been completed successfully, false otherwise.
   */
  public boolean isSuccess() {
    return this.state == State.COMPLETED;
  }

  /**
   * Is the job still running?
   *
   * @return True if the job is still running, false otherwise.
   */
  public boolean isRunning() {
    return this.state == State.RUNNING;
  }

  @Override
  public String toString() {
    if (this.error.isPresent()) {
      return this.state + "(" + this.error.get() + ")";
    } else {
      return this.state.toString();
    }
  }

  /**
   * The state the computation could be in.
   */
  private enum State {
    INIT,
    SUBMITTED,
    RUNNING,
    COMPLETED,
    FAILED,
    FORCE_CLOSED
  }
}
