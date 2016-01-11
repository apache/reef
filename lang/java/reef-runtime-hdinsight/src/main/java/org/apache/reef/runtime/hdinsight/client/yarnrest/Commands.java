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
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.io.IOException;
import java.io.StringWriter;

/**
 * Commands used to start an Application Master via the
 * YARN Resource Manger REST API.
 * For detailed information, please refer to
 * https://hadoop.apache.org/docs/r2.6.0/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html
 */
public final class Commands {

  public static final String DEFAULT_COMMAND = "";

  private static final String COMMANDS = "commands";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private String command = DEFAULT_COMMAND;

  @JsonProperty(Constants.COMMAND)
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_EMPTY)
    public String getCommand() {
    return this.command;
  }

  public void setCommand(final String command) {
    this.command = command;
  }

  @Override
    public String toString() {
    final StringWriter writer = new StringWriter();
    final String objectString;
    try {
      OBJECT_MAPPER.writeValue(writer, this);
      objectString = writer.toString();
    } catch (final IOException e) {
      throw new RuntimeException("Exception while serializing Commands: " + e);
    }

    return COMMANDS + objectString;
  }
}
