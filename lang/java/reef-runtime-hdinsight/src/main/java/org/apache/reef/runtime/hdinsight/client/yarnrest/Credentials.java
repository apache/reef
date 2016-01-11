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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents credentials for an application in the YARN REST API.
 * For detailed information, please refer to
 * https://hadoop.apache.org/docs/r2.6.0/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html
 */
public class Credentials {

  private static final String CREDENTIALS = "credentials";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private Map<String, List<StringEntry>> tokens = new HashMap<>();
  private Map<String, List<StringEntry>> secrets = new HashMap<>();

  public Credentials() {
    this.tokens.put(Constants.ENTRY, new ArrayList<StringEntry>());
    this.secrets.put(Constants.ENTRY, new ArrayList<StringEntry>());
  }

  public Credentials addSecret(final String key, final String value) {
    if (!this.secrets.containsKey(Constants.ENTRY)) {
      this.secrets.put(Constants.ENTRY, new ArrayList<StringEntry>());
    }
    this.secrets.get(Constants.ENTRY).add(new StringEntry(key, value));
    return this;
  }

  public Credentials addToken(final String key, final String value) {
    if (!this.tokens.containsKey(Constants.ENTRY)) {
      this.tokens.put(Constants.ENTRY, new ArrayList<StringEntry>());
    }
    this.tokens.get(Constants.ENTRY).add(new StringEntry(key, value));
    return this;
  }

  @JsonProperty(Constants.SECRETS)
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
    public Map<String, List<StringEntry>> getSecrets() {
    return this.secrets;
  }

  public Credentials setSecrets(final Map<String, List<StringEntry>> secrets) {
    this.secrets = secrets;
    return this;
  }

  @JsonProperty(Constants.TOKENS)
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
    public Map<String, List<StringEntry>> getTokens() {
    return this.tokens;
  }

  public Credentials setTokens(final Map<String, List<StringEntry>> tokens) {
    this.tokens = tokens;
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
      throw new RuntimeException("Exception while serializing Credentials: " + e);
    }

    return CREDENTIALS + objectString;
  }
}
