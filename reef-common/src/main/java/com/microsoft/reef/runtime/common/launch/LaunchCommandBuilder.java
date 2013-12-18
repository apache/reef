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

  /**
   * Set the name of the configuration file for the Launcher. This file is assumed to exist in the working directory of
   * the process launched with this command line.
   *
   * @param configurationFileName
   * @return this
   */
  public LaunchCommandBuilder setConfigurationFileName(final String configurationFileName);

  /**
   * Names a file to which stdout will be redirected.
   *
   * @param standardOut
   * @return this
   */
  public LaunchCommandBuilder setStandardOut(final String standardOut);

  /**
   * Names a file to which stderr will be redirected.
   *
   * @param standardErr
   * @return this
   */
  public LaunchCommandBuilder setStandardErr(final String standardErr);


}
