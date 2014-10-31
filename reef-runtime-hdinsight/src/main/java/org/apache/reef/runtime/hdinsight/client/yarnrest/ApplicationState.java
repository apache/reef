/**
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
package org.apache.reef.runtime.hdinsight.client.yarnrest;

/**
 * Created by marku_000 on 2014-06-30.
 */
public class ApplicationState {
  private String progress;
  private String queue;
  private String trackingUI;
  private String state;
  private String amContainerLogs;
  private String applicationType;
  private int runningContainers;
  private int allocatedMB;
  private long elapsedTime;
  private String amHostHttpAddress;
  private String id;
  private String finalStatus;
  private String trackingUrl;
  private int allocatedVCores;
  private long finishedTime;
  private String applicationTags;
  private String name;
  private long clusterId;
  private String user;
  private String diagnostics;
  private long startedTime;

  public String getProgress() {
    return progress;
  }

  public void setProgress(String progress) {
    this.progress = progress;
  }

  public String getQueue() {
    return queue;
  }

  public void setQueue(String queue) {
    this.queue = queue;
  }

  public String getTrackingUI() {
    return trackingUI;
  }

  public void setTrackingUI(String trackingUI) {
    this.trackingUI = trackingUI;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public String getAmContainerLogs() {
    return amContainerLogs;
  }

  public void setAmContainerLogs(String amContainerLogs) {
    this.amContainerLogs = amContainerLogs;
  }

  public String getApplicationType() {
    return applicationType;
  }

  public void setApplicationType(String applicationType) {
    this.applicationType = applicationType;
  }

  public int getRunningContainers() {
    return runningContainers;
  }

  public void setRunningContainers(int runningContainers) {
    this.runningContainers = runningContainers;
  }

  public int getAllocatedMB() {
    return allocatedMB;
  }

  public void setAllocatedMB(int allocatedMB) {
    this.allocatedMB = allocatedMB;
  }

  public long getElapsedTime() {
    return elapsedTime;
  }

  public void setElapsedTime(long elapsedTime) {
    this.elapsedTime = elapsedTime;
  }

  public String getAmHostHttpAddress() {
    return amHostHttpAddress;
  }

  public void setAmHostHttpAddress(String amHostHttpAddress) {
    this.amHostHttpAddress = amHostHttpAddress;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getFinalStatus() {
    return finalStatus;
  }

  public void setFinalStatus(String finalStatus) {
    this.finalStatus = finalStatus;
  }

  public String getTrackingUrl() {
    return trackingUrl;
  }

  public void setTrackingUrl(String trackingUrl) {
    this.trackingUrl = trackingUrl;
  }

  public int getAllocatedVCores() {
    return allocatedVCores;
  }

  public void setAllocatedVCores(int allocatedVCores) {
    this.allocatedVCores = allocatedVCores;
  }

  public long getFinishedTime() {
    return finishedTime;
  }

  public void setFinishedTime(long finishedTime) {
    this.finishedTime = finishedTime;
  }

  public String getApplicationTags() {
    return applicationTags;
  }

  public void setApplicationTags(String applicationTags) {
    this.applicationTags = applicationTags;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public long getClusterId() {
    return clusterId;
  }

  public void setClusterId(long clusterId) {
    this.clusterId = clusterId;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getDiagnostics() {
    return diagnostics;
  }

  public void setDiagnostics(String diagnostics) {
    this.diagnostics = diagnostics;
  }

  public long getStartedTime() {
    return startedTime;
  }

  public void setStartedTime(long startedTime) {
    this.startedTime = startedTime;
  }
}
