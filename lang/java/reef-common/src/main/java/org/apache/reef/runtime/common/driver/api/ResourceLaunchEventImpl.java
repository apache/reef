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
package org.apache.reef.runtime.common.driver.api;

import org.apache.reef.runtime.common.files.FileResource;
import org.apache.reef.runtime.common.launch.ProcessType;
import org.apache.reef.tang.Configuration;
import org.apache.reef.util.BuilderUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Default POJO implementation of ResourceLaunchEvent.
 * Use newBuilder to construct an instance.
 */
public final class ResourceLaunchEventImpl implements ResourceLaunchEvent {

  private final String identifier;
  private final String remoteId;
  private final Configuration evaluatorConf;
  private final ProcessType type;
  private final List<FileResource> fileList;

  private ResourceLaunchEventImpl(final Builder builder) {
    this.identifier = BuilderUtils.notNull(builder.identifier);
    this.remoteId = BuilderUtils.notNull(builder.remoteId);
    this.evaluatorConf = BuilderUtils.notNull(builder.evaluatorConf);
    this.type = BuilderUtils.notNull(builder.type);
    this.fileList = BuilderUtils.notNull(builder.fileList);
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
  public Configuration getEvaluatorConf() {
    return evaluatorConf;
  }

  @Override
  public ProcessType getType() {
    return type;
  }

  @Override
  public List<FileResource> getFileList() {
    return fileList;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder used to create ResourceLaunchEvent instances.
   */
  public static final class Builder implements org.apache.reef.util.Builder<ResourceLaunchEvent> {
    private String identifier;
    private String remoteId;
    private Configuration evaluatorConf;
    private ProcessType type;
    private List<FileResource> fileList = new ArrayList<>();

    /**
     * @see ResourceLaunchEvent#getIdentifier()
     */
    public Builder setIdentifier(final String identifier) {
      this.identifier = identifier;
      return this;
    }

    /**
     * @see ResourceLaunchEvent#getRemoteId()
     */
    public Builder setRemoteId(final String remoteId) {
      this.remoteId = remoteId;
      return this;
    }

    /**
     * @see ResourceLaunchEvent#getEvaluatorConf()
     */
    public Builder setEvaluatorConf(final Configuration evaluatorConf) {
      this.evaluatorConf = evaluatorConf;
      return this;
    }

    /**
     * @see ResourceLaunchEvent#getType()
     */
    public Builder setType(final ProcessType type) {
      this.type = type;
      return this;
    }

    /**
     * Add an entry to the fileList
     * @see ResourceLaunchEvent#getFileList()
     */
    public Builder addFile(final FileResource file) {
      this.fileList.add(file);
      return this;
    }

    @Override
    public ResourceLaunchEvent build() {
      return new ResourceLaunchEventImpl(this);
    }
  }
}
