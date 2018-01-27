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
 * Represents a the details of a local resource used
 * in an HDInsight job submission.
 * For detailed information, please refer to
 * https://hadoop.apache.org/docs/r2.6.0/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html
 */
public final class LocalResource {

  public static final String TYPE_FILE = "FILE";
  public static final String TYPE_ARCHIVE = "ARCHIVE";
  public static final String TYPE_PATTERN = "PATTERN";
  public static final String VISIBILITY_PUBLIC = "PUBLIC";
  public static final String VISIBILITY_PRIVATE = "PRIVATE";
  public static final String VISIBILITY_APPLICATION = "APPLICATION";
  private static final String LOCAL_RESOURCE = "localResource";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private String resource;
  private String type;
  private String visibility;
  private long size;
  private long timestamp;

  @JsonProperty(Constants.RESOURCE)
  public String getResource() {
    return this.resource;
  }

  public LocalResource setResource(final String resource) {
    this.resource = resource;
    return this;
  }

  @JsonProperty(Constants.TYPE)
  public String getType() {
    return this.type;
  }

  public LocalResource setType(final String type) {
    this.type = type;
    return this;
  }

  @JsonProperty(Constants.VISIBILITY)
  public String getVisibility() {
    return this.visibility;
  }

  public LocalResource setVisibility(final String visibility) {
    this.visibility = visibility;
    return this;
  }

  @JsonProperty(Constants.SIZE)
  public long getSize() {
    return this.size;
  }

  public LocalResource setSize(final long size) {
    this.size = size;
    return this;
  }

  @JsonProperty(Constants.TIMESTAMP)
  public long getTimestamp() {
    return this.timestamp;
  }

  public LocalResource setTimestamp(final long timestamp) {
    this.timestamp = timestamp;
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
      throw new RuntimeException("Exception while serializing LocalResource: " + e);
    }

    return LOCAL_RESOURCE + objectString;
  }
}
