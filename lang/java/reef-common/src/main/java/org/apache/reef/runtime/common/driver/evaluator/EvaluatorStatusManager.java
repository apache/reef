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
package org.apache.reef.runtime.common.driver.evaluator;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages Status of a single Evaluator.
 */
@DriverSide
@Private
final class EvaluatorStatusManager {
  private static final Logger LOG = Logger.getLogger(EvaluatorStatusManager.class.getName());
  /**
   * The state managed.
   */
  private EvaluatorState state = EvaluatorState.ALLOCATED;

  @Inject
  private EvaluatorStatusManager() {
    LOG.log(Level.FINE, "Instantiated 'EvaluatorStatusManager'");
  }

  private static boolean isLegal(final EvaluatorState from, final EvaluatorState to) {
    if (from == to) {
      return true;
    }

    switch(from) {
    case ALLOCATED: {
      switch(to) {
      case SUBMITTED:
      case DONE:
      case CLOSING:
      case FAILED:
        return true;
      case KILLED:
      case RUNNING:
        break;
      default:
        throw new RuntimeException("Unknown state: " + to);
      }
    }
    case SUBMITTED: {
      switch(to) {
      case RUNNING:
      case DONE:
      case CLOSING:
      case FAILED:
        return true;
      case ALLOCATED:
      case KILLED:
        break;
      default:
        throw new RuntimeException("Unknown state: " + to);
      }
    }
    case RUNNING: {
      switch(to) {
      case DONE:
      case CLOSING:
      case FAILED:
        return true;
      case ALLOCATED:
      case SUBMITTED:
      case KILLED:
        break;
      default:
        throw new RuntimeException("Unknown state: " + to);
      }
    }
    case CLOSING: {
      switch(to) {
      case KILLED:
      case DONE:
      case FAILED:
        return true;
      case ALLOCATED:
      case SUBMITTED:
      case RUNNING:
        break;
      default:
        throw new RuntimeException("Unknown state: " + to);
      }
    }
    case DONE:
    case FAILED:
    case KILLED:
      break;
    default:
      throw new RuntimeException("Unknown state: " + from);
    }

    LOG.warning("Illegal evaluator state transition from " + from + " to " + to + ".");
    return false;
  }

  private static boolean isDoneOrFailedOrKilled(final EvaluatorState state) {
    return state == EvaluatorState.DONE ||
           state == EvaluatorState.FAILED ||
           state == EvaluatorState.KILLED;
  }

  synchronized void setRunning() {
    this.setState(EvaluatorState.RUNNING);
  }

  synchronized void setSubmitted() {
    this.setState(EvaluatorState.SUBMITTED);
  }

  synchronized void setClosing() {
    this.setState(EvaluatorState.CLOSING);
  }

  synchronized void setDone() {
    this.setState(EvaluatorState.DONE);
  }

  synchronized void setFailed() {
    this.setState(EvaluatorState.FAILED);
  }

  synchronized void setKilled() {
    this.setState(EvaluatorState.KILLED);
  }

  synchronized boolean isRunning() {
    return this.state.equals(EvaluatorState.RUNNING);
  }

  synchronized boolean isDoneOrFailedOrKilled() {
    return isDoneOrFailedOrKilled(this.state);
  }

  synchronized boolean isAllocatedOrSubmittedOrRunning() {
    return this.state == EvaluatorState.ALLOCATED ||
           this.state == EvaluatorState.SUBMITTED ||
           this.state == EvaluatorState.RUNNING;
  }

  synchronized boolean isSubmitted() {
    return EvaluatorState.SUBMITTED == this.state;
  }

  synchronized boolean isAllocated() {
    return EvaluatorState.ALLOCATED == this.state;
  }

  synchronized boolean isFailedOrKilled() {
    return EvaluatorState.FAILED == this.state || EvaluatorState.KILLED == this.state;
  }

  synchronized boolean isClosing() {
    return EvaluatorState.CLOSING == this.state;
  }

  @Override
  public synchronized String toString() {
    return this.state.toString();
  }

  private synchronized void setState(final EvaluatorState state) {
    if (!isLegal(this.state, state)) {
      throw new IllegalStateException("Illegal state transition from '" + this.state + "' to '" + state + "'");
    }
    this.state = state;
  }
}
