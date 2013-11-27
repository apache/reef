package com.microsoft.reef.runtime.common.launch;

import java.util.List;

/**
 * Used to build the launch command for REEF processes.
 */
public interface LaunchCommandBuilder {


  /**
   * @return the launch command line
   */
  public List<String> build();

  public LaunchCommandBuilder setErrorHandlerRID(final String errorHandlerRID);

  public LaunchCommandBuilder setLaunchID(final String launchID);

  public LaunchCommandBuilder setMemory(final int megaBytes);

  public LaunchCommandBuilder setConfigurationPath(final String evaluatorConfigurationPath);

  public LaunchCommandBuilder setStandardOut(final String standardOut);

  public LaunchCommandBuilder setStandardErr(final String standardOut);



}
