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

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.StringWriter;

/**
 * Represents the response to an application ID request to
 * the YARN Resource Manager via the YARN REST API.
 * For detailed information, please refer to
 * https://hadoop.apache.org/docs/r2.6.0/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class ApplicationID {

  private static final String APPLICATION_ID = "applicationId";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private String applicationId;
  private Resource resource;

  @JsonProperty(Constants.APPLICATION_ID)
  public String getApplicationId() {
    return applicationId;
  }

  public void setApplicationId(final String applicationId) {
    this.applicationId = applicationId;
  }

  @JsonProperty(Constants.MAXIMUM_RESOURCE_CAPABILITY)
  public Resource getResource() {
    return resource;
  }

  public void setResource(final Resource resource) {
    this.resource = resource;
  }

  @Override
  public String toString() {
    final StringWriter writer = new StringWriter();
    final String objectString;
    try {
      OBJECT_MAPPER.writeValue(writer, this);
      objectString = writer.toString();
    } catch (final IOException e) {
      throw new RuntimeException("Exception while serializing ApplicationID: " + e);
    }

    return APPLICATION_ID + objectString;
  }
}
