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
package org.apache.reef.runtime.hdinsight.client.yarnrest;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.StringWriter;

/**
 * An object representing the state of an application,
 * used to deserialize queries for an application/list of applications
 * to the Resource Manager on HDInsight via the YARN REST API.
 * For detailed information, please refer to
 * https://hadoop.apache.org/docs/r2.6.0/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class ApplicationState {

  private static final String APPLICATION_STATE = "applicationState";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private String progress;
  private String queue;
  private String trackingUI;
  private String state;
  private String amContainerLogs;
  private int runningContainers;
  private int allocatedMB;
  private long elapsedTime;
  private String amHostHttpAddress;
  private String id;
  private String finalStatus;
  private String trackingUrl;
  private int allocatedVCores;
  private long finishedTime;
  private String name;
  private String applicationType;
  private String clusterId;
  private String user;
  private String diagnostics;
  private long startedTime;
  private long memorySeconds;
  private long vCoreSeconds;

  @JsonProperty(Constants.FINISHED_TIME)
  public long getFinishedTime() {
    return finishedTime;
  }

  public void setFinishedTime(final long finishedTime) {
    this.finishedTime = finishedTime;
  }

  @JsonProperty(Constants.AM_CONTAINER_LOGS)
  public String getAmContainerLogs() {
    return amContainerLogs;
  }

  public void setAmContainerLogs(final String amContainerLogs) {
    this.amContainerLogs = amContainerLogs;
  }

  @JsonProperty(Constants.TRACKING_UI)
  public String getTrackingUI() {
    return trackingUI;
  }

  public void setTrackingUI(final String trackingUI) {
    this.trackingUI = trackingUI;
  }

  @JsonProperty(Constants.STATE)
  public String getState() {
    return state;
  }

  public void setState(final String state) {
    this.state = state;
  }

  @JsonProperty(Constants.USER)
  public String getUser() {
    return user;
  }

  public void setUser(final String user) {
    this.user = user;
  }

  @JsonProperty(Constants.ID)
  public String getId() {
    return id;
  }

  public void setId(final String id) {
    this.id = id;
  }

  @JsonProperty(Constants.CLUSTER_ID)
  public String getClusterId() {
    return clusterId;
  }

  public void setClusterId(final String clusterId) {
    this.clusterId = clusterId;
  }

  @JsonProperty(Constants.FINAL_STATUS)
  public String getFinalStatus() {
    return finalStatus;
  }

  public void setFinalStatus(final String finalStatus) {
    this.finalStatus = finalStatus;
  }

  @JsonProperty(Constants.AM_HOST_HTTP_ADDRESS)
  public String getAmHostHttpAddress() {
    return amHostHttpAddress;
  }

  public void setAmHostHttpAddress(final String amHostHttpAddress) {
    this.amHostHttpAddress = amHostHttpAddress;
  }

  @JsonProperty(Constants.PROGRESS)
  public String getProgress() {
    return progress;
  }

  public void setProgress(final String progress) {
    this.progress = progress;
  }

  @JsonProperty(Constants.NAME)
  public String getName() {
    return name;
  }

  public void setName(final String name) {
    this.name = name;
  }

  @JsonProperty(Constants.RESPONSE_APPLICATION_TYPE)
  public String getApplicationType() {
    return applicationType;
  }

  public void setApplicationType(final String applicationType) {
    this.applicationType = applicationType;
  }

  @JsonProperty(Constants.STARTED_TIME)
  public long getStartedTime() {
    return startedTime;
  }

  public void setStartedTime(final long startedTime) {
    this.startedTime = startedTime;
  }

  @JsonProperty(Constants.ELAPSED_TIME)
  public long getElapsedTime() {
    return elapsedTime;
  }

  public void setElapsedTime(final long elapsedTime) {
    this.elapsedTime = elapsedTime;
  }

  @JsonProperty(Constants.DIAGNOSTICS)
  public String getDiagnostics() {
    return diagnostics;
  }

  public void setDiagnostics(final String diagnostics) {
    this.diagnostics = diagnostics;
  }

  @JsonProperty(Constants.TRACKING_URL)
  public String getTrackingUrl() {
    return trackingUrl;
  }

  public void setTrackingUrl(final String trackingUrl) {
    this.trackingUrl = trackingUrl;
  }

  @JsonProperty(Constants.QUEUE)
  public String getQueue() {
    return queue;
  }

  public void setQueue(final String queue) {
    this.queue = queue;
  }

  @JsonProperty(Constants.ALLOCATED_MB)
  public int getAllocatedMB() {
    return allocatedMB;
  }

  public void setAllocatedMB(final int allocatedMB) {
    this.allocatedMB = allocatedMB;
  }

  @JsonProperty(Constants.ALLOCATED_VCORES)
  public int getAllocatedVCores() {
    return allocatedVCores;
  }

  public void setAllocatedVCores(final int allocatedVCores) {
    this.allocatedVCores = allocatedVCores;
  }

  @JsonProperty(Constants.RUNNING_CONTAINERS)
  public int getRunningContainers() {
    return runningContainers;
  }

  public void setRunningContainers(final int runningContainers) {
    this.runningContainers = runningContainers;
  }

  @JsonProperty(Constants.MEMORY_SECONDS)
  public long getMemorySeconds() {
    return memorySeconds;
  }

  public void setMemorySeconds(final long memorySeconds) {
    this.memorySeconds = memorySeconds;
  }

  @JsonProperty(Constants.VCORE_SECONDS)
  public long getVCoreSeconds() {
    return vCoreSeconds;
  }

  @SuppressWarnings("checkstyle:hiddenfield")
  public void setVCoreSeconds(final long vCoreSeconds) {
    this.vCoreSeconds = vCoreSeconds;
  }

  @Override
  public String toString() {
    final StringWriter writer = new StringWriter();
    final String objectString;
    try {
      OBJECT_MAPPER.writeValue(writer, this);
      objectString = writer.toString();
    } catch (final IOException e) {
      throw new RuntimeException("Exception while serializing ApplicationState: " + e);
    }

    return APPLICATION_STATE + objectString;
  }
}
