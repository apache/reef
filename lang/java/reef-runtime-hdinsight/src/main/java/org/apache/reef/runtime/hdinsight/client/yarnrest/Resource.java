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

import java.io.IOException;
import java.io.StringWriter;

/**
 * Represents the resoure field in the YARN REST API.
 * For detailed information, please refer to
 * https://hadoop.apache.org/docs/r2.6.0/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html
 */
public final class Resource {

  private static final String RESOURCE = "resource";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private int memory;
  private int vCores;

  @JsonProperty(Constants.MEMORY)
  public int getMemory() {
    return this.memory;
  }

  public Resource setMemory(final int memory) {
    this.memory = memory;
    return this;
  }

  @JsonProperty(Constants.VCORES)
  public int getvCores() {
    return this.vCores;
  }

  public Resource setvCores(final int vCores) {
    this.vCores = vCores;
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
      throw new RuntimeException("Exception while serializing Resource: " + e);
    }

    return RESOURCE + objectString;
  }
}
