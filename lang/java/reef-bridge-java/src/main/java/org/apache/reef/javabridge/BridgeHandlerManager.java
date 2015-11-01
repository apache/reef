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
package org.apache.reef.javabridge;

import org.apache.reef.annotations.audience.Interop;
import org.apache.reef.annotations.audience.Private;

/**
 * A class that holds all handles to the .NET side.
 * USED BY UNMANAGED CODE! PLEASE DO NOT CHANGE ANY FUNCTION SIGNATURES
 * UNLESS YOU KNOW WHAT YOU ARE DOING!
 */
@Private
@Interop(CppFiles = { "JavaClrBridge.cpp" }, CsFiles = { "BridgeHandlerManager.cs" })
public final class BridgeHandlerManager {
  private long allocatedEvaluatorHandler = 0;
  private long activeContextHandler = 0;
  private long taskMessageHandler = 0;
  private long failedTaskHandler = 0;
  private long failedEvaluatorHandler = 0;
  private long httpServerEventHandler = 0;
  private long completedTaskHandler = 0;
  private long runningTaskHandler = 0;
  private long suspendedTaskHandler = 0;
  private long completedEvaluatorHandler = 0;
  private long closedContextHandler = 0;
  private long failedContextHandler = 0;
  private long contextMessageHandler = 0;
  private long driverRestartActiveContextHandler = 0;
  private long driverRestartRunningTaskHandler = 0;
  private long driverRestartCompletedHandler = 0;
  private long driverRestartFailedEvaluatorHandler = 0;
  private long progressProvider = 0;

  public BridgeHandlerManager() {
  }

  public long getAllocatedEvaluatorHandler() {
    return allocatedEvaluatorHandler;
  }

  public void setAllocatedEvaluatorHandler(final long allocatedEvaluatorHandler) {
    this.allocatedEvaluatorHandler = allocatedEvaluatorHandler;
  }

  public long getActiveContextHandler() {
    return activeContextHandler;
  }

  public void setActiveContextHandler(final long activeContextHandler) {
    this.activeContextHandler = activeContextHandler;
  }

  public long getTaskMessageHandler() {
    return taskMessageHandler;
  }

  public void setTaskMessageHandler(final long taskMessageHandler) {
    this.taskMessageHandler = taskMessageHandler;
  }

  public long getFailedTaskHandler() {
    return failedTaskHandler;
  }

  public void setFailedTaskHandler(final long failedTaskHandler) {
    this.failedTaskHandler = failedTaskHandler;
  }

  public long getFailedEvaluatorHandler() {
    return failedEvaluatorHandler;
  }

  public void setFailedEvaluatorHandler(final long failedEvaluatorHandler) {
    this.failedEvaluatorHandler = failedEvaluatorHandler;
  }

  public long getHttpServerEventHandler() {
    return httpServerEventHandler;
  }

  public void setHttpServerEventHandler(final long httpServerEventHandler) {
    this.httpServerEventHandler = httpServerEventHandler;
  }

  public long getCompletedTaskHandler() {
    return completedTaskHandler;
  }

  public void setCompletedTaskHandler(final long completedTaskHandler) {
    this.completedTaskHandler = completedTaskHandler;
  }

  public long getRunningTaskHandler() {
    return runningTaskHandler;
  }

  public void setRunningTaskHandler(final long runningTaskHandler) {
    this.runningTaskHandler = runningTaskHandler;
  }

  public long getSuspendedTaskHandler() {
    return suspendedTaskHandler;
  }

  public void setSuspendedTaskHandler(final long suspendedTaskHandler) {
    this.suspendedTaskHandler = suspendedTaskHandler;
  }

  public long getCompletedEvaluatorHandler() {
    return completedEvaluatorHandler;
  }

  public void setCompletedEvaluatorHandler(final long completedEvaluatorHandler) {
    this.completedEvaluatorHandler = completedEvaluatorHandler;
  }

  public long getClosedContextHandler() {
    return closedContextHandler;
  }

  public void setClosedContextHandler(final long closedContextHandler) {
    this.closedContextHandler = closedContextHandler;
  }

  public long getFailedContextHandler() {
    return failedContextHandler;
  }

  public void setFailedContextHandler(final long failedContextHandler) {
    this.failedContextHandler = failedContextHandler;
  }

  public long getContextMessageHandler() {
    return contextMessageHandler;
  }

  public void setContextMessageHandler(final long contextMessageHandler) {
    this.contextMessageHandler = contextMessageHandler;
  }

  public long getDriverRestartActiveContextHandler() {
    return driverRestartActiveContextHandler;
  }

  public void setDriverRestartActiveContextHandler(final long driverRestartActiveContextHandler) {
    this.driverRestartActiveContextHandler = driverRestartActiveContextHandler;
  }

  public long getDriverRestartRunningTaskHandler() {
    return driverRestartRunningTaskHandler;
  }

  public void setDriverRestartRunningTaskHandler(final long driverRestartRunningTaskHandler) {
    this.driverRestartRunningTaskHandler = driverRestartRunningTaskHandler;
  }

  public long getDriverRestartCompletedHandler() {
    return driverRestartCompletedHandler;
  }

  public void setDriverRestartCompletedHandler(final long driverRestartCompletedHandler) {
    this.driverRestartCompletedHandler = driverRestartCompletedHandler;
  }

  public long getDriverRestartFailedEvaluatorHandler() {
    return driverRestartFailedEvaluatorHandler;
  }

  public void setDriverRestartFailedEvaluatorHandler(final long driverRestartFailedEvaluatorHandler) {
    this.driverRestartFailedEvaluatorHandler = driverRestartFailedEvaluatorHandler;
  }

  public long getProgressProvider() {
    return progressProvider;
  }

  public void setProgressProvider(final long progressProvider) {
    this.progressProvider = progressProvider;
  }
}
