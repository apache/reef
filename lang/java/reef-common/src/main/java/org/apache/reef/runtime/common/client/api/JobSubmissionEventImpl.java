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
package org.apache.reef.runtime.common.client.api;

import org.apache.reef.runtime.common.files.FileResource;
import org.apache.reef.util.BuilderUtils;
import org.apache.reef.util.Optional;

import java.util.ArrayList;
import java.util.List;

public final class JobSubmissionEventImpl implements JobSubmissionEvent {
  private final String identifier;
  private final String remoteId;
  private final String configuration;
  private final String userName;
  private final List<FileResource> globalFileList;
  private final List<FileResource> localFileList;
  private final Optional<Integer> driverMemory;
  private final Optional<Integer> priority;
  private final Optional<String> queue;

  private JobSubmissionEventImpl(final Builder builder) {
    this.identifier = BuilderUtils.notNull(builder.identifier);
    this.remoteId = BuilderUtils.notNull(builder.remoteId);
    this.configuration = BuilderUtils.notNull(builder.configuration);
    this.userName = BuilderUtils.notNull(builder.userName);
    this.globalFileList = BuilderUtils.notNull(builder.globalFileList);
    this.localFileList = BuilderUtils.notNull(builder.localFileList);
    this.driverMemory = Optional.ofNullable(builder.driverMemory);
    this.priority = Optional.ofNullable(builder.priority);
    this.queue = Optional.ofNullable(builder.queue);
  }

  @Override
  public String getIdentifier() {
    return identifier;
  }

  @Override
  public String getRemoteId() {
    return remoteId;
  }

  @Override
  public String getConfiguration() {
    return configuration;
  }

  @Override
  public String getUserName() {
    return userName;
  }

  @Override
  public List<FileResource> getGlobalFileList() {
    return globalFileList;
  }

  @Override
  public List<FileResource> getLocalFileList() {
    return localFileList;
  }

  @Override
  public Optional<Integer> getDriverMemory() {
    return driverMemory;
  }

  @Override
  public Optional<Integer> getPriority() {
    return priority;
  }

  @Override
  public Optional<String> getQueue() {
    return queue;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder implements org.apache.reef.util.Builder<JobSubmissionEvent> {
    private String identifier;
    private String remoteId;
    private String configuration;
    private String userName;
    private List<FileResource> globalFileList = new ArrayList<>();
    private List<FileResource> localFileList = new ArrayList<>();
    private Integer driverMemory;
    private Integer priority;
    private String queue;

    public Builder setIdentifier(final String identifier) {
      this.identifier = identifier;
      return this;
    }

    public Builder setRemoteId(final String remoteId) {
      this.remoteId = remoteId;
      return this;
    }

    public Builder setConfiguration(final String configuration) {
      this.configuration = configuration;
      return this;
    }

    public Builder setUserName(final String userName) {
      this.userName = userName;
      return this;
    }

    public Builder addGlobalFile(final FileResource globalFile) {
      this.globalFileList.add(globalFile);
      return this;
    }

    public Builder addLocalFile(final FileResource localFile) {
      this.localFileList.add(localFile);
      return this;
    }

    public Builder setDriverMemory(final Integer driverMemory) {
      this.driverMemory = driverMemory;
      return this;
    }

    public Builder setPriority(final Integer priority) {
      this.priority = priority;
      return this;
    }

    public Builder setQueue(final String queue) {
      this.queue = queue;
      return this;
    }

    @Override
    public JobSubmissionEvent build() {
      return new JobSubmissionEventImpl(this);
    }
  }
}
