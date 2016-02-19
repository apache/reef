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

import org.apache.reef.util.BuilderUtils;

/**
 * Default POJO implementation of ResourceReleaseEvent.
 * Use newBuilder to construct an instance.
 */
public final class ResourceReleaseEventImpl implements ResourceReleaseEvent {

  private final String identifier;
  private final String runtimeName;

  private ResourceReleaseEventImpl(final Builder builder) {
    this.identifier = BuilderUtils.notNull(builder.identifier);
    this.runtimeName = BuilderUtils.notNull(builder.runtimeName);
  }

  @Override
  public String getIdentifier() {
    return identifier;
  }

  @Override
  public String getRuntimeName() {
    return runtimeName;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder used to create ResourceReleaseEvent instances.
   */
  public static final class Builder implements org.apache.reef.util.Builder<ResourceReleaseEvent> {

    private String identifier;
    private String runtimeName;
    /**
     * @see ResourceReleaseEvent#getIdentifier()
     */
    public Builder setIdentifier(final String identifier) {
      this.identifier = identifier;
      return this;
    }

    /**
     * @see ResourceReleaseEvent#getRuntimeName()
     */
    public Builder setRuntimeName(final String runtimeName) {
      this.runtimeName = runtimeName;
      return this;
    }

    @Override
    public ResourceReleaseEvent build() {
      return new ResourceReleaseEventImpl(this);
    }
  }
}
