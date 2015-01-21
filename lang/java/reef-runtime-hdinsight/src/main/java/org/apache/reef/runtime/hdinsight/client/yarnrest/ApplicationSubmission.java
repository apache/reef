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
 * Represents an ApplicationSubmission to the YARN REST API.
 */
public final class ApplicationSubmission {

  public static final String DEFAULT_QUEUE = "default";
  private String queue = DEFAULT_QUEUE;

  public static final String DEFAULT_PRIORITY = "3";
  private String priority = DEFAULT_PRIORITY;

  public static final String DEFAULT_MAX_ATTEMPTS = "1";
  private String maxAppAttempts = DEFAULT_MAX_ATTEMPTS;

  public static final String DEFAULT_APPLICATION_TYPE = "YARN";
  private String applicationType = DEFAULT_APPLICATION_TYPE;

  public static final String DEFAULT_KEEP_CONTAINERS = "false";
  private String keepContainers = DEFAULT_KEEP_CONTAINERS;

  public static final String DEFAULT_IS_UNMANAGED_AM = "false";
  private String isUnmanagedAM = DEFAULT_IS_UNMANAGED_AM;

  public static final String DEFAULT_CANCEL_TOKENS_WHEN_COMPLETE = "true";
  private String cancelTokensWhenComplete = DEFAULT_CANCEL_TOKENS_WHEN_COMPLETE;

  private String applicationId;
  private String applicationName;
  private ContainerInfo containerInfo;
  private Resource resource;

  public String getApplicationId() {
    return applicationId;
  }

  public ApplicationSubmission setApplicationId(String applicationId) {
    this.applicationId = applicationId;
    return this;
  }

  public String getApplicationName() {
    return applicationName;
  }

  public ApplicationSubmission setApplicationName(String applicationName) {
    this.applicationName = applicationName;
    return this;
  }

  public String getApplicationType() {
    return applicationType;
  }

  public ApplicationSubmission setApplicationType(String applicationType) {
    this.applicationType = applicationType;
    return this;
  }

  public String isCancelTokensWhenComplete() {
    return cancelTokensWhenComplete;
  }

  public ApplicationSubmission setCancelTokensWhenComplete(String cancelTokensWhenComplete) {
    this.cancelTokensWhenComplete = cancelTokensWhenComplete;
    return this;
  }

  public ContainerInfo getContainerInfo() {
    return containerInfo;
  }

  public ApplicationSubmission setContainerInfo(ContainerInfo containerInfo) {
    this.containerInfo = containerInfo;
    return this;
  }

  public String isUnmanagedAM() {
    return isUnmanagedAM;
  }

  public ApplicationSubmission setUnmanagedAM(String isUnmanagedAM) {
    this.isUnmanagedAM = isUnmanagedAM;
    return this;
  }

  public String isKeepContainers() {
    return keepContainers;
  }

  public ApplicationSubmission setKeepContainers(String keepContainers) {
    this.keepContainers = keepContainers;
    return this;
  }

  public String getMaxAppAttempts() {
    return maxAppAttempts;
  }

  public ApplicationSubmission setMaxAppAttempts(String maxAppAttempts) {
    this.maxAppAttempts = maxAppAttempts;
    return this;
  }

  public String getPriority() {
    return priority;
  }

  public ApplicationSubmission setPriority(String priority) {
    this.priority = priority;
    return this;
  }

  public String getQueue() {
    return queue;
  }

  public ApplicationSubmission setQueue(String queue) {
    this.queue = queue;
    return this;
  }

  public Resource getResource() {
    return resource;
  }

  public ApplicationSubmission setResource(Resource resource) {
    this.resource = resource;
    return this;
  }

  @Override
  public String toString() {
    return "ApplicationSubmission{" +
        "queue='" + queue + '\'' +
        ", priority=" + priority +
        ", maxAppAttempts=" + maxAppAttempts +
        ", applicationType='" + applicationType + '\'' +
        ", keepContainers=" + keepContainers +
        ", applicationId='" + applicationId + '\'' +
        ", applicationName='" + applicationName + '\'' +
        ", containerInfo=" + containerInfo +
        ", isUnmanagedAM=" + isUnmanagedAM +
        ", cancelTokensWhenComplete=" + cancelTokensWhenComplete +
        ", resource=" + resource +
        '}';
  }
}
