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

import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents an Application Submission object to the YARN REST API.
 * Contains all the information needed to submit an application to the
 * Resource Manager.
 * For detailed information, please refer to
 * https://hadoop.apache.org/docs/r2.6.0/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html
 */
public final class ApplicationSubmission {

  public static final String DEFAULT_QUEUE = "default";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String APPLICATION_SUBMISSION = "applicationSubmission";
  private String queue = DEFAULT_QUEUE;

  public static final int DEFAULT_PRIORITY = 3;
  private int priority = DEFAULT_PRIORITY;

  public static final int DEFAULT_MAX_APP_ATTEMPTS = 1;
  private int maxAppAttempts = DEFAULT_MAX_APP_ATTEMPTS;

  public static final String DEFAULT_APPLICATION_TYPE = "YARN";
  private String applicationType = DEFAULT_APPLICATION_TYPE;

  public static final boolean DEFAULT_KEEP_CONTAINERS_ACROSS_APPLICATION_ATTEMPTS = false;
  private boolean keepContainers = DEFAULT_KEEP_CONTAINERS_ACROSS_APPLICATION_ATTEMPTS;

  public static final boolean DEFAULT_UNMANAGED_AM = false;
  private boolean isUnmanagedAM = DEFAULT_UNMANAGED_AM;

  private String applicationId;
  private String applicationName;
  private AmContainerSpec amContainerSpec;
  private Resource resource;
  private List<String> applicationTags = new ArrayList<>();

  @JsonProperty(Constants.APPLICATION_ID)
  public String getApplicationId() {
    return applicationId;
  }

  public ApplicationSubmission setApplicationId(final String applicationId) {
    this.applicationId = applicationId;
    return this;
  }

  @JsonProperty(Constants.APPLICATION_NAME)
  public String getApplicationName() {
    return applicationName;
  }

  public ApplicationSubmission setApplicationName(final String applicationName) {
    this.applicationName = applicationName;
    return this;
  }

  @JsonProperty(Constants.APPLICATION_TYPE)
  public String getApplicationType() {
    return applicationType;
  }

  public ApplicationSubmission setApplicationType(final String applicationType) {
    this.applicationType = applicationType;
    return this;
  }

  @JsonProperty(Constants.AM_CONTAINER_SPEC)
  public AmContainerSpec getAmContainerSpec() {
    return amContainerSpec;
  }

  public ApplicationSubmission setAmContainerSpec(final AmContainerSpec amContainerSpec) {
    this.amContainerSpec = amContainerSpec;
    return this;
  }

  @JsonProperty(Constants.UNMANAGED_AM)
  public boolean isUnmanagedAM() {
    return isUnmanagedAM;
  }

  @SuppressWarnings("checkstyle:hiddenfield")
  public ApplicationSubmission setUnmanagedAM(final boolean isUnmanagedAM) {
    this.isUnmanagedAM = isUnmanagedAM;
    return this;
  }

  @JsonProperty(Constants.KEEP_CONTAINERS_ACROSS_APPLICATION_ATTEMPTS)
  public boolean isKeepContainers() {
    return keepContainers;
  }

  public ApplicationSubmission setKeepContainers(final boolean keepContainers) {
    this.keepContainers = keepContainers;
    return this;
  }

  @JsonProperty(Constants.MAX_APP_ATTEMPTS)
  public int getMaxAppAttempts() {
    return maxAppAttempts;
  }

  public ApplicationSubmission setMaxAppAttempts(final int maxAppAttempts) {
    this.maxAppAttempts = maxAppAttempts;
    return this;
  }

  @JsonProperty(Constants.PRIORITY)
  public int getPriority() {
    return priority;
  }

  public ApplicationSubmission setPriority(final int priority) {
    this.priority = priority;
    return this;
  }

  @JsonProperty(Constants.QUEUE)
  public String getQueue() {
    return queue;
  }

  public ApplicationSubmission setQueue(final String queue) {
    this.queue = queue;
    return this;
  }

  @JsonProperty(Constants.RESOURCE)
  public Resource getResource() {
    return resource;
  }

  public ApplicationSubmission setResource(final Resource resource) {
    this.resource = resource;
    return this;
  }

  public ApplicationSubmission addApplicationTag(final String tag) {
    this.applicationTags.add(tag);
    return this;
  }

  @JsonProperty(Constants.APPLICATION_TAGS)
  @JsonSerialize(include = JsonSerialize.Inclusion.NON_EMPTY)
  public List<String> getApplicationTags() {
    return this.applicationTags;
  }

  public ApplicationSubmission setApplicationTags(final List<String> applicationTags) {
    this.applicationTags = applicationTags;
    return this;
  }

  @Override
  public String toString() {
    final StringWriter writer = new StringWriter();
    final String objectString;
    try {
      OBJECT_MAPPER.writeValue(writer, this);
      objectString = writer.toString();
    } catch (final IOException e) {
      throw new RuntimeException("Exception while serializing ApplicationSubmission: " + e);
    }

    return APPLICATION_SUBMISSION + objectString;
  }
}
