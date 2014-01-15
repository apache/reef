package com.microsoft.reef.runtime.common.launch;

import org.apache.commons.lang.StringUtils;

import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A builder for the command line to launch a CLR Evaluator.
 */
public class CLRLaunchCommandBuilder implements LaunchCommandBuilder {
  private static final Logger LOG = Logger.getLogger(CLRLaunchCommandBuilder.class.getName());
  private static final String EVALUATOR_PATH = "Evaluator.exe";


  private String standardErrPath = null;
  private String standardOutPath = null;
  private String errorHandlerRID = null;
  private String launchID = null;
  private int megaBytes = 0;
  private String evaluatorConfigurationPath = null;

  @Override
  public List<String> build() {
    final List<String> result = new LinkedList<>();
    result.add(EVALUATOR_PATH);
    result.add(errorHandlerRID);
    result.add(evaluatorConfigurationPath);
    if ((null != this.standardOutPath) && (!standardOutPath.isEmpty())) {
      result.add(">" + this.standardOutPath);
    }
    if ((null != this.standardErrPath) && (!standardErrPath.isEmpty())) {
      result.add("2>" + this.standardErrPath);
    }
    LOG.log(Level.INFO, "Launch Exe: {0}", StringUtils.join(result, ' '));
    return result;
  }

  @Override
  public CLRLaunchCommandBuilder setErrorHandlerRID(final String errorHandlerRID) {
    this.errorHandlerRID = errorHandlerRID;
    return this;
  }

  @Override
  public CLRLaunchCommandBuilder setLaunchID(final String launchID) {
    this.launchID = launchID;
    return this;
  }

  @Override
  public CLRLaunchCommandBuilder setMemory(final int megaBytes) {
    this.megaBytes = megaBytes;
    return this;
  }

  @Override
  public CLRLaunchCommandBuilder setConfigurationFileName(final String configurationFileName) {
    this.evaluatorConfigurationPath = configurationFileName;
    return this;
  }

  @Override
  public CLRLaunchCommandBuilder setStandardOut(final String standardOut) {
    this.standardOutPath = standardOut;
    return this;
  }

  @Override
  public CLRLaunchCommandBuilder setStandardErr(final String standardErr) {
    this.standardErrPath = standardErr;
    return this;
  }
}
