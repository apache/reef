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
package org.apache.reef.runtime.common.driver.api;

import org.apache.reef.driver.evaluator.EvaluatorProcess;
import org.apache.reef.runtime.common.files.FileResource;
import org.apache.reef.runtime.common.files.FileResourceImpl;
import org.apache.reef.runtime.common.files.FileType;
import org.apache.reef.tang.Configuration;
import org.apache.reef.util.BuilderUtils;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

/**
 * Default POJO implementation of ResourceLaunchEvent.
 * Use newBuilder to construct an instance.
 */
public final class ResourceLaunchEventImpl implements ResourceLaunchEvent {

  private final String identifier;
  private final String remoteId;
  private final Configuration evaluatorConf;
  private final EvaluatorProcess process;
  private final Set<FileResource> fileSet;
  private final String runtimeName;

  private ResourceLaunchEventImpl(final Builder builder) {
    this.identifier = BuilderUtils.notNull(builder.identifier);
    this.remoteId = BuilderUtils.notNull(builder.remoteId);
    this.evaluatorConf = BuilderUtils.notNull(builder.evaluatorConf);
    this.process = BuilderUtils.notNull(builder.process);
    this.fileSet = BuilderUtils.notNull(builder.fileSet);
    this.runtimeName = BuilderUtils.notNull(builder.runtimeName);
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
  public EvaluatorProcess getProcess() {
    return process;
  }

  @Override
  public Set<FileResource> getFileSet() {
    return fileSet;
  }

  @Override
  public String getRuntimeName() {
    return runtimeName;
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
    private EvaluatorProcess process;
    private Set<FileResource> fileSet = new HashSet<>();
    private String runtimeName;

    /**
     * @see ResourceLaunchEvent#getIdentifier()
     */
    public Builder setIdentifier(final String identifier) {
      this.identifier = identifier;
      return this;
    }

    /**
     * @see ResourceLaunchEvent#getRuntimeName()
     */
    public Builder setRuntimeName(final String runtimeName) {
      this.runtimeName = runtimeName;
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
     * @see ResourceLaunchEvent#getProcess()
     */
    public Builder setProcess(final EvaluatorProcess process) {
      this.process = process;
      return this;
    }

    /**
     * Add an entry to the fileSet.
     *
     * @see ResourceLaunchEvent#getFileSet()
     */
    public Builder addFile(final FileResource file) {
      this.fileSet.add(file);
      return this;
    }

    /**
     * Utility method that adds files to the fileSet.
     *
     * @param files the files to add.
     * @return this
     * @see ResourceLaunchEvent#getFileSet()
     */
    public Builder addFiles(final Iterable<File> files) {
      for (final File file : files) {
        this.addFile(FileResourceImpl.newBuilder()
            .setName(file.getName())
            .setPath(file.getPath())
            .setType(FileType.PLAIN)
            .build());
      }
      return this;
    }

    /**
     * Utility method that adds Libraries to the fileSet.
     *
     * @param files the files to add.
     * @return this
     * @see ResourceLaunchEvent#getFileSet()
     */
    public Builder addLibraries(final Iterable<File> files) {
      for (final File file : files) {
        this.addFile(FileResourceImpl.newBuilder()
            .setName(file.getName())
            .setPath(file.getPath())
            .setType(FileType.LIB)
            .build());
      }
      return this;
    }

    @Override
    public ResourceLaunchEvent build() {
      return new ResourceLaunchEventImpl(this);
    }
  }
}
