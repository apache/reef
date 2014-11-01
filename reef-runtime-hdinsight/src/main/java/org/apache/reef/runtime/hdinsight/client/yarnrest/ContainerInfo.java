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


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a ContainerInfo in the YARN REST APIs.
 */
public final class ContainerInfo {

  public static final String DEFAULT_SERVICE_DATA = null;
  private String serviceData = DEFAULT_SERVICE_DATA;

  public static final String DEFAULT_TOKENS = "";
  private String tokens = DEFAULT_TOKENS;

  public static final String DEFAULT_ACLS = null;
  private String acls = DEFAULT_ACLS;

  private List<String> commands = new ArrayList<>();
  private Map<String, EnvironmentEntry> environment = new HashMap<>();
  private Map<String, LocalResourcesEntry> localResources = new HashMap<>();

  /**
   * Adds an environment variable.
   *
   * @param key   the name of the variable
   * @param value the value it shall take
   * @return this
   */
  public ContainerInfo addEnvironment(final String key, final String value) {
    this.environment.put("entry", new EnvironmentEntry(key, value));
    return this;
  }

  /**
   * Adds a command to the command list to be executed
   *
   * @param command
   * @return this
   */
  public ContainerInfo addCommand(final String command) {
    this.commands.add(command);
    return this;
  }

  public ContainerInfo addFileResource(final String key, final FileResource fileResource) {
    this.localResources.put("entry", new LocalResourcesEntry(key, fileResource));
    return this;
  }

  public String getServiceData() {
    return this.serviceData;
  }

  public ContainerInfo setServiceData(final String serviceData) {
    this.serviceData = serviceData;
    return this;
  }

  public String getTokens() {
    return this.tokens;
  }

  public ContainerInfo setTokens(final String tokens) {
    this.tokens = tokens;
    return this;
  }

  public String getAcls() {
    return this.acls;
  }

  public ContainerInfo setAcls(final String acls) {
    this.acls = acls;
    return this;
  }

  public Map<String, EnvironmentEntry> getEnvironment() {
    return this.environment;
  }

  public void setEnvironment(final Map<String, EnvironmentEntry> environment) {
    this.environment = environment;
  }

  public List<String> getCommands() {
    return this.commands;
  }

  public ContainerInfo setCommands(final List<String> commands) {
    this.commands = commands;
    return this;
  }

  public Map<String, LocalResourcesEntry> getLocalResources() {
    return this.localResources;
  }

  public ContainerInfo setLocalResources(final Map<String, LocalResourcesEntry> localResources) {
    this.localResources = localResources;
    return this;
  }
}
