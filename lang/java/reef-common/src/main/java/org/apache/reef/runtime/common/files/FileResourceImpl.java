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
package org.apache.reef.runtime.common.files;

import org.apache.reef.util.BuilderUtils;

/**
 * Default POJO implementation of FileResource.
 * Use newBuilder to construct an instance.
 */
public final class FileResourceImpl implements FileResource {
  private final FileType type;
  private final String name;
  private final String path;

  private FileResourceImpl(final Builder builder) {
    this.type = BuilderUtils.notNull(builder.type);
    this.name = BuilderUtils.notNull(builder.name);
    this.path = BuilderUtils.notNull(builder.path);
  }

  @Override
  public FileType getType() {
    return type;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getPath() {
    return path;
  }

  @Override
  public String toString() {
    return String.format("FileResource: {%s:%s=%s}", this.name, this.type, this.path);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder used to create FileResource instances.
   */
  public static final class Builder implements org.apache.reef.util.Builder<FileResource> {
    private FileType type;
    private String name;
    private String path;

    /**
     * @see FileResource#getType()
     */
    public Builder setType(final FileType type) {
      this.type = type;
      return this;
    }

    /**
     * @see FileResource#getName()
     */
    public Builder setName(final String name) {
      this.name = name;
      return this;
    }

    /**
     * @see FileResource#getPath()
     */
    public Builder setPath(final String path) {
      this.path = path;
      return this;
    }

    @Override
    public FileResource build() {
      return new FileResourceImpl(this);
    }
  }
}
