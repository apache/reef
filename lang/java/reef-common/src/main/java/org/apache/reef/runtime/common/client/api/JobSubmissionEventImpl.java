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
package org.apache.reef.runtime.common.client.api;

import org.apache.reef.runtime.common.files.FileResource;
import org.apache.reef.tang.Configuration;
import org.apache.reef.util.BuilderUtils;
import org.apache.reef.util.Optional;

import java.util.HashSet;
import java.util.Set;

/**
 * Default POJO implementation of JobSubmissionEvent.
 * Use newBuilder to construct an instance.
 */
public final class JobSubmissionEventImpl implements JobSubmissionEvent {
  private final String identifier;
  private final String remoteId;
  private final Configuration configuration;
  private final String userName;
  private final Set<FileResource> globalFileSet;
  private final Set<FileResource> localFileSet;
  private final Optional<Integer> driverMemory;
  private final Optional<Integer> priority;
  private final Optional<String> queue;

  private JobSubmissionEventImpl(final Builder builder) {
    this.identifier = BuilderUtils.notNull(builder.identifier);
    this.remoteId = BuilderUtils.notNull(builder.remoteId);
    this.configuration = BuilderUtils.notNull(builder.configuration);
    this.userName = BuilderUtils.notNull(builder.userName);
    this.globalFileSet = BuilderUtils.notNull(builder.globalFileSet);
    this.localFileSet = BuilderUtils.notNull(builder.localFileSet);
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
  public Configuration getConfiguration() {
    return configuration;
  }

  @Override
  public String getUserName() {
    return userName;
  }

  @Override
  public Set<FileResource> getGlobalFileSet() {
    return globalFileSet;
  }

  @Override
  public Set<FileResource> getLocalFileSet() {
    return localFileSet;
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

  /**
   * Builder used to create JobSubmissionEvent instances.
   */
  public static final class Builder implements org.apache.reef.util.Builder<JobSubmissionEvent> {
    private String identifier;
    private String remoteId;
    private Configuration configuration;
    private String userName;
    private Set<FileResource> globalFileSet = new HashSet<>();
    private Set<FileResource> localFileSet = new HashSet<>();
    private Integer driverMemory;
    private Integer priority;
    private String queue;

    /**
     * @see JobSubmissionEvent#getIdentifier()
     */
    public Builder setIdentifier(final String identifier) {
      this.identifier = identifier;
      return this;
    }

    /**
     * @see JobSubmissionEvent#getRemoteId()
     */
    public Builder setRemoteId(final String remoteId) {
      this.remoteId = remoteId;
      return this;
    }

    /**
     * @see JobSubmissionEvent#getConfiguration()
     */
    public Builder setConfiguration(final Configuration configuration) {
      this.configuration = configuration;
      return this;
    }

    /**
     * @see JobSubmissionEvent#getUserName()
     */
    public Builder setUserName(final String userName) {
      this.userName = userName;
      return this;
    }

    /**
     * Add an entry to the globalFileSet.
     * @see JobSubmissionEvent#getGlobalFileSet()
     */
    public Builder addGlobalFile(final FileResource globalFile) {
      this.globalFileSet.add(globalFile);
      return this;
    }

    /**
     * Add an entry to the localFileSet.
     * @see JobSubmissionEvent#getLocalFileSet()
     */
    public Builder addLocalFile(final FileResource localFile) {
      this.localFileSet.add(localFile);
      return this;
    }

    /**
     * @see JobSubmissionEvent#getDriverMemory()
     */
    public Builder setDriverMemory(final Integer driverMemory) {
      this.driverMemory = driverMemory;
      return this;
    }

    /**
     * @see JobSubmissionEvent#getPriority()
     */
    public Builder setPriority(final Integer priority) {
      this.priority = priority;
      return this;
    }

    /**
     * @see JobSubmissionEvent#getQueue()
     * @deprecated in 0.12. Use org.apache.reef.runtime.yarn.client.YarnDriverConfiguration#QUEUE instead.
     */
    @Deprecated
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
