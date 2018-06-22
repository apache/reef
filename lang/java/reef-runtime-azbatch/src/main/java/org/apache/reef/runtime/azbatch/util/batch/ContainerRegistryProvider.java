package org.apache.reef.runtime.azbatch.util.batch;

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
}
