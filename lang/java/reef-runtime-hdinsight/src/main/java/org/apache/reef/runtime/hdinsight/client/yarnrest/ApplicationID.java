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
package org.apache.reef.runtime.hdinsight.client.yarnrest;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Represents the response to an application ID request.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class ApplicationID {

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
    return Constants.APPLICATION_ID + "{" +
        Constants.ID + "='" + applicationId + '\'' +
        ", " + Constants.MAXIMUM_RESOURCE_CAPABILITY + "=" + resource +
        '}';
  }
}
