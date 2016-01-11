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
 * Represents the specifications for an application master
 * container. Used in job submission to the Resource Manager
 * via the YARN REST API.
 * For detailed information, please refer to
 * https://hadoop.apache.org/docs/r2.6.0/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html
 */
public final class AmContainerSpec {

  public static final String ACLS_VIEW_APP = "VIEW_APP";
  public static final String ACLS_MODIFY_APP = "MODIFY_APP";

  private static final String AM_CONTAINER_SPEC = "AmContainerSpec";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private Commands commands = new Commands();
  private Map<String, List<StringEntry>> environment = new HashMap<>();
  private Map<String, List<LocalResourcesEntry>> localResources = new HashMap<>();
  private Map<String, List<StringEntry>> applicationAcls = new HashMap<>();
  private Map<String, List<StringEntry>> serviceData = new HashMap<>();
  private Credentials credentials;

  public AmContainerSpec(){
    this.localResources.put(Constants.ENTRY, new ArrayList<LocalResourcesEntry>());
    this.environment.put(Constants.ENTRY, new ArrayList<StringEntry>());
    this.applicationAcls.put(Constants.ENTRY, new ArrayList<StringEntry>());
    this.serviceData.put(Constants.ENTRY, new ArrayList<StringEntry>());
  }

  public AmContainerSpec addEnvironment(final String key, final String value) {
    if (!this.environment.containsKey(Constants.ENTRY)) {
      this.environment.put(Constants.ENTRY, new ArrayList<StringEntry>());
    }
    this.environment.get(Constants.ENTRY).add(new StringEntry(key, value));
    return this;
  }

  public AmContainerSpec addLocalResource(final String key, final LocalResource localResource) {
    if (!this.localResources.containsKey(Constants.ENTRY)) {
      this.localResources.put(Constants.ENTRY, new ArrayList<LocalResourcesEntry>());
    }
    this.localResources.get(Constants.ENTRY).add(new LocalResourcesEntry(key, localResource));
    return this;
  }

  public AmContainerSpec addApplicationAcl(final String key, final String value) {
    if (!this.applicationAcls.containsKey(Constants.ENTRY)) {
      this.applicationAcls.put(Constants.ENTRY, new ArrayList<StringEntry>());
    }
    this.applicationAcls.get(Constants.ENTRY).add(new StringEntry(key, value));
    return this;
  }

  public AmContainerSpec setCommand(final String command) {
    this.commands.setCommand(command);
    return this;
  }

  public AmContainerSpec addServiceData(final String key, final String value) {
    if (!this.serviceData.containsKey(Constants.ENTRY)) {
      this.serviceData.put(Constants.ENTRY, new ArrayList<StringEntry>());
    }
    this.serviceData.get(Constants.ENTRY).add(new StringEntry(key, value));
    return this;
  }

  @JsonProperty(Constants.CREDENTIALS)
  @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
  public Credentials getCredentials() {
    return this.credentials;
  }

  public AmContainerSpec setCredentials(final Credentials credentials) {
    this.credentials = credentials;
    return this;
  }

  @JsonProperty(Constants.SERVICE_DATA)
  @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
  public Map<String, List<StringEntry>> getServiceData() {
    return this.serviceData;
  }

  public AmContainerSpec setServiceData(final Map<String, List<StringEntry>> serviceData) {
    this.serviceData = serviceData;
    return this;
  }

  @JsonProperty(Constants.APPLICATION_ACLS)
  @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
  public Map<String, List<StringEntry>> getApplicationAcls() {
    return this.applicationAcls;
  }

  public AmContainerSpec setApplicationAcls(final Map<String, List<StringEntry>> applicationAcls) {
    this.applicationAcls = applicationAcls;
    return this;
  }

  @JsonProperty(Constants.ENVIRONMENT)
  @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
  public Map<String, List<StringEntry>> getEnvironment() {
    return this.environment;
  }

  public void setEnvironment(final Map<String, List<StringEntry>> environment) {
    this.environment = environment;
  }

  @JsonProperty(Constants.COMMANDS)
  public Commands getCommands() {
    return this.commands;
  }

  public AmContainerSpec setCommands(final Commands commands) {
    this.commands = commands;
    return this;
  }

  @JsonProperty(Constants.LOCAL_RESOURCES)
  public Map<String, List<LocalResourcesEntry>> getLocalResources() {
    return this.localResources;
  }

  public AmContainerSpec setLocalResources(final Map<String, List<LocalResourcesEntry>> localResources) {
    this.localResources = localResources;
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
      throw new RuntimeException("Exception while serializing AmContainerSpec: " + e);
    }

    return AM_CONTAINER_SPEC + objectString;
  }
}
