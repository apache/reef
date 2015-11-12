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
package org.apache.reef.io.watcher;

import org.apache.reef.annotations.Unstable;
import org.apache.reef.driver.parameters.*;
import org.apache.reef.io.watcher.param.EventStreams;
import org.apache.reef.tang.formats.*;
import org.apache.reef.wake.time.Clock;

/**
 * ConfigurationModule for Watcher.
 */
@Unstable
public final class WatcherConfiguration extends ConfigurationModuleBuilder {

  /**
   * Types of EventStream where events subscribed by Watcher will be reported.
   */
  public static final OptionalImpl<EventStream> EVENT_STREAMS = new OptionalImpl<>();

  public static final ConfigurationModule CONF = new WatcherConfiguration()
      .bindSetEntry(EventStreams.class, EVENT_STREAMS)
      .bindSetEntry(Clock.RuntimeStartHandler.class, Watcher.DriverRuntimeStartHandler.class)
      .bindSetEntry(Clock.StartHandler.class, Watcher.DriverStartHandler.class)
      .bindSetEntry(Clock.StopHandler.class, Watcher.DriverStopHandler.class)
      .bindSetEntry(Clock.RuntimeStopHandler.class, Watcher.DriverRuntimeStopHandler.class)
      .bindSetEntry(ServiceContextActiveHandlers.class, Watcher.ContextActiveHandler.class)
      .bindSetEntry(ServiceContextClosedHandlers.class, Watcher.ContextClosedHandler.class)
      .bindSetEntry(ServiceContextFailedHandlers.class, Watcher.ContextFailedHandler.class)
      .bindSetEntry(ServiceTaskFailedHandlers.class, Watcher.TaskFailedHandler.class)
      .bindSetEntry(ServiceTaskCompletedHandlers.class, Watcher.TaskCompletedHandler.class)
      .bindSetEntry(ServiceTaskMessageHandlers.class, Watcher.TaskMessageHandler.class)
      .bindSetEntry(ServiceTaskRunningHandlers.class, Watcher.TaskRunningHandler.class)
      .bindSetEntry(ServiceTaskSuspendedHandlers.class, Watcher.TaskSuspendedHandler.class)
      .bindSetEntry(ServiceEvaluatorAllocatedHandlers.class, Watcher.EvaluatorAllocatedHandler.class)
      .bindSetEntry(ServiceEvaluatorFailedHandlers.class, Watcher.EvaluatorFailedHandler.class)
      .bindSetEntry(ServiceEvaluatorCompletedHandlers.class, Watcher.EvaluatorCompletedHandler.class)
      .build();

  /**
   * Construct a WatcherConfiguration.
   */
  private WatcherConfiguration() {
  }
}
