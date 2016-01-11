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
 * A response object used in deserialization when querying
 * the Resource Manager for an application via the YARN REST API.
 * For detailed information, please refer to
 * https://hadoop.apache.org/docs/r2.6.0/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class ApplicationResponse {

  private static final String APPLICATION_RESPONSE = "applicationResponse";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private ApplicationState app;

  @JsonProperty(Constants.APP)
  public ApplicationState getApp() {
    return this.app;
  }

  public void setApp(final ApplicationState app) {
    this.app = app;
  }

  public ApplicationState getApplicationState() {
    return app;
  }

  @Override
  public String toString() {
    final StringWriter writer = new StringWriter();
    final String objectString;
    try {
      OBJECT_MAPPER.writeValue(writer, this);
      objectString = writer.toString();
    } catch (final IOException e) {
      throw new RuntimeException("Exception while serializing ApplicationResponse: " + e);
    }

    return APPLICATION_RESPONSE + objectString;
  }
}
