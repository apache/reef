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
package org.apache.reef.runtime.azbatch.util.batch;

import com.microsoft.azure.batch.protocol.models.ContainerRegistry;
import org.apache.commons.lang.StringUtils;
import org.apache.reef.runtime.azbatch.parameters.ContainerImageName;
import org.apache.reef.runtime.azbatch.parameters.ContainerRegistryPassword;
import org.apache.reef.runtime.azbatch.parameters.ContainerRegistryServer;
import org.apache.reef.runtime.azbatch.parameters.ContainerRegistryUsername;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * Class that holds the settings of the Azure container service settings.
 */
public final class ContainerRegistryProvider {

  private final String containerRegistryServer;
  private final String containerRegistryUsername;
  private final String containerRegistryPassword;
  private final String containerImageName;

  @Inject
  private ContainerRegistryProvider(
      @Parameter(ContainerRegistryServer.class) final String containerRegistryServer,
      @Parameter(ContainerRegistryUsername.class) final String containerRegistryUsername,
      @Parameter(ContainerRegistryPassword.class) final String containerRegistryPassword,
      @Parameter(ContainerImageName.class) final String containerImageName
  ) {
    this.containerRegistryServer = containerRegistryServer;
    this.containerRegistryUsername = containerRegistryUsername;
    this.containerRegistryPassword = containerRegistryPassword;
    this.containerImageName = containerImageName;
  }

  public boolean isValid() {
    return !StringUtils.isEmpty(this.containerRegistryServer);
  }

  public String getContainerRegistryServer() {
    return this.containerRegistryServer;
  }

  public String getContainerRegistryUsername() {
    return this.containerRegistryUsername;
  }

  public String getContainerRegistryPassword() {
    return this.containerRegistryPassword;
  }

  public String getContainerImageName() {
    return this.containerImageName;
  }

  public ContainerRegistry getContainerRegistry() {
    if (!this.isValid()) {
      return null;
    }

    return new ContainerRegistry()
        .withRegistryServer(this.getContainerRegistryServer())
        .withUserName(this.getContainerRegistryUsername())
        .withPassword(this.getContainerRegistryPassword());
  }
}
