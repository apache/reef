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
  private final Optional<Integer> driverCpuCores;
  private final Optional<Integer> priority;
  private final Optional<String> queue;
  private final Optional<Boolean> preserveEvaluators;
  private final Optional<Integer> maxApplicationSubmissions;

  private JobSubmissionEventImpl(final Builder builder) {
    this.identifier = BuilderUtils.notNull(builder.identifier);
    this.remoteId = BuilderUtils.notNull(builder.remoteId);
    this.configuration = BuilderUtils.notNull(builder.configuration);
    this.userName = BuilderUtils.notNull(builder.userName);
    this.globalFileSet = BuilderUtils.notNull(builder.globalFileSet);
    this.localFileSet = BuilderUtils.notNull(builder.localFileSet);
    this.driverMemory = Optional.ofNullable(builder.driverMemory);
    this.driverCpuCores = Optional.ofNullable(builder.driverCpuCores);
    this.priority = Optional.ofNullable(builder.priority);
    this.preserveEvaluators = Optional.ofNullable(builder.preserveEvaluators);
    this.queue = Optional.ofNullable(builder.queue);
    this.maxApplicationSubmissions = Optional.ofNullable(builder.maxApplicationSubmissions);
  }

  @Override
  public String toString() {
    return String.format(
        "Job Submission :: user:%s id:%s remote:%s mem:%s queue:%s priority:%s" +
        " preserve evaluators:%s max submissions:%s global:%s local:%s",
        this.userName, this.identifier, this.remoteId, this.driverMemory, this.queue, this.priority,
        this.preserveEvaluators, this.maxApplicationSubmissions, this.globalFileSet, this.localFileSet);
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
  public Optional<Integer> getDriverCPUCores(){
    return driverCpuCores;
  }

  @Override
  public Optional<Integer> getPriority() {
    return priority;
  }

  @Override
  public Optional<Boolean> getPreserveEvaluators() {
    return preserveEvaluators;
  }

  @Override
  public Optional<Integer> getMaxApplicationSubmissions() {
    return maxApplicationSubmissions;
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
    private Integer driverCpuCores;
    private Integer priority;
    private String queue;
    private Boolean preserveEvaluators;
    private Integer maxApplicationSubmissions;

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
     * @see JobSubmissionEvent#getDriverCPUCores()
     */
    public Builder setDriverCpuCores(final Integer driverCpuCores){
      this.driverCpuCores = driverCpuCores;
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
     * @see JobSubmissionEvent#getPreserveEvaluators()
     */
    public Builder setPreserveEvaluators(final Boolean preserveEvaluators) {
      this.preserveEvaluators = preserveEvaluators;
      return this;
    }

    /**
     * @see JobSubmissionEvent#getMaxApplicationSubmissions()
     */
    public Builder setMaxApplicationSubmissions(final Integer maxApplicationSubmissions) {
      this.maxApplicationSubmissions = maxApplicationSubmissions;
      return this;
    }

    @Override
    public JobSubmissionEvent build() {
      return new JobSubmissionEventImpl(this);
    }
  }
}
