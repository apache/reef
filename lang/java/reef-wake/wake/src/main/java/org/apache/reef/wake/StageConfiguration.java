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
package org.apache.reef.wake;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.wake.rx.Observer;

import java.util.concurrent.ExecutorService;

/**
 * Configuration options for Wake Stage.
 */
public final class StageConfiguration {

  /**
   * The stage name.
   */
  @NamedParameter(doc = "The stage name.")
  public static final class StageName implements Name<String> {
  }

  /**
   * The event handler for the stage.
   */
  @NamedParameter(doc = "The event handler for the stage.")
  public static final class StageHandler implements Name<EventHandler<?>> {
  }

  /**
   * The error handler for the stage.
   */
  @NamedParameter(doc = "The error handler for the stage.")
  public static final class ErrorHandler implements Name<EventHandler<Throwable>> {
  }

  /**
   * The number of threads for the stage.
   */
  @NamedParameter(doc = "The number of threads for the stage.")
  public static final class NumberOfThreads implements Name<Integer> {
  }

  /**
   * The capacity for the stage.
   */
  @NamedParameter(doc = "The capacity for the stage.")
  public static final class Capacity implements Name<Integer> {
  }

  /**
   * The executor service for the stage.
   */
  @NamedParameter(doc = "The executor service for the stage.")
  public static final class StageExecutorService implements Name<ExecutorService> {
  }

  /**
   * The initial delay for periodic events of the timer stage.
   */
  @NamedParameter(doc = "The initial delay for periodic events of the timer stage.")
  public static final class TimerInitialDelay implements Name<Long> {
  }

  /**
   * The period for periodic events of the timer stage.
   */
  @NamedParameter(doc = "The period for periodic events of the timer stage.")
  public static final class TimerPeriod implements Name<Long> {
  }

  /**
   * The observer for the stage.
   */
  @NamedParameter(doc = "The observer for the stage.")
  public static final class StageObserver implements Name<Observer<?>> {
  }

}
