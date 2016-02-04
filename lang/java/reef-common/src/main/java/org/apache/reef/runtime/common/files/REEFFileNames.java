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
package org.apache.reef.runtime.common.files;

import net.jcip.annotations.Immutable;

import javax.inject.Inject;
import java.io.File;

/**
 * Access to the various places things go according to the REEF file system standard.
 */
@Immutable
public final class REEFFileNames {

  private static final String REEF_BASE_FOLDER = "reef";
  private static final String GLOBAL_FOLDER = "global";
  private static final String GLOBAL_FOLDER_PATH = REEF_BASE_FOLDER + '/' + GLOBAL_FOLDER;
  private static final String LOCAL_FOLDER = "local";
  private static final String LOCAL_FOLDER_PATH = REEF_BASE_FOLDER + '/' + LOCAL_FOLDER;
  private static final String DRIVER_CONFIGURATION_NAME = "driver.conf";
  private static final String DRIVER_CONFIGURATION_PATH =
      LOCAL_FOLDER_PATH + '/' + DRIVER_CONFIGURATION_NAME;
  private static final String EVALUATOR_CONFIGURATION_NAME = "evaluator.conf";
  private static final String EVALUATOR_CONFIGURATION_PATH =
      LOCAL_FOLDER_PATH + '/' + EVALUATOR_CONFIGURATION_NAME;
  private static final String JAR_FILE_SUFFIX = ".jar";
  private static final String JOB_FOLDER_PREFIX = "reef-job-";
  private static final String EVALUATOR_FOLDER_PREFIX = "reef-evaluator-";
  private static final String DRIVER_STDERR = "driver.stderr";
  private static final String DRIVER_STDOUT = "driver.stdout";
  private static final String EVALUATOR_STDERR = "evaluator.stderr";
  private static final String EVALUATOR_STDOUT = "evaluator.stdout";
  private static final String DRIVER_HTTP_ENDPOINT_FILE_NAME = "DriverHttpEndpoint.txt";
  private static final String BRIDGE_EXE_NAME = "Org.Apache.REEF.Bridge.exe";
  private static final String SECURITY_TOKEN_IDENTIFIER_FILE = "SecurityTokenId";
  private static final String SECURITY_TOKEN_PASSWORD_FILE = "SecurityTokenPwd";
  private static final String YARN_BOOTSTRAP_APP_PARAM_FILE = "yarn-app-parameters.json";
  private static final String YARN_BOOTSTRAP_JOB_PARAM_FILE = "yarn-job-parameters.json";

  @Inject
  public REEFFileNames() {
  }


  /**
   * @return the filename of the CPP DLL for the bridge.
   */
  public String getBridgeExeName() {
    return BRIDGE_EXE_NAME;
  }

  /**
   * reef/BRIDGE_EXE_NAME.
   *
   * @return the File pointing to the EXE that is the clr driver launcher.
   */
  public File getDriverLauncherExeFile() {
    return new File(getREEFFolderName(), getBridgeExeName());
  }

  /**
   * The name of the REEF folder inside of the working directory of an Evaluator or Driver.
   */
  public String getREEFFolderName() {
    return REEF_BASE_FOLDER;
  }

  /**
   * @return the folder und which all REEF files are stored.
   */
  public File getREEFFolder() {
    return new File(getREEFFolderName());
  }


  /**
   * @return the name of the folder inside of REEF_BASE_FOLDER that houses the global files.
   */
  public String getGlobalFolderName() {
    return GLOBAL_FOLDER;
  }

  /**
   * @return the path to the global folder: REEF_BASE_FOLDER/GLOBAL_FOLDER
   */
  public String getGlobalFolderPath() {
    return GLOBAL_FOLDER_PATH;
  }

  /**
   * @return the folder holding the files global to all containers.
   */
  public File getGlobalFolder() {
    return new File(getREEFFolder(), getGlobalFolderName());
  }


  /**
   * @return the name of the folder inside of REEF_BASE_FOLDER that houses the local files.
   */
  public String getLocalFolderName() {
    return LOCAL_FOLDER;
  }

  /**
   * @return the path to the local folder: REEF_BASE_FOLDER/LOCAL_FOLDER
   */
  public String getLocalFolderPath() {
    return LOCAL_FOLDER_PATH;
  }

  /**
   * @return the folder holding the files local to this container.
   */
  public File getLocalFolder() {
    return new File(getREEFFolder(), getLocalFolderName());
  }


  /**
   * The name under which the driver configuration will be stored in REEF_BASE_FOLDER/LOCAL_FOLDER.
   */
  public String getDriverConfigurationName() {
    return DRIVER_CONFIGURATION_NAME;
  }

  /**
   * @return The path to the driver configuration: GLOBAL_FOLDER/LOCAL_FOLDER/DRIVER_CONFIGURATION_NAME
   */
  public String getDriverConfigurationPath() {
    return DRIVER_CONFIGURATION_PATH;
  }

  /**
   * @return The name under which the driver configuration will be stored in REEF_BASE_FOLDER/LOCAL_FOLDER
   */
  public String getEvaluatorConfigurationName() {
    return EVALUATOR_CONFIGURATION_NAME;
  }

  /**
   * @return the path to the evaluator configuration.
   */
  public String getEvaluatorConfigurationPath() {
    return EVALUATOR_CONFIGURATION_PATH;
  }

  /**
   * @return The suffix used for JAR files, including the "."
   */
  public String getJarFileSuffix() {
    return JAR_FILE_SUFFIX;
  }

  /**
   * The prefix used whenever REEF is asked to create a job folder, on (H)DFS or locally.
   * <p>
   * This prefix is also used with JAR files created to represent a job.
   */
  public String getJobFolderPrefix() {
    return JOB_FOLDER_PREFIX;
  }

  /**
   * @return The name used within the current working directory of the driver to redirect standard error to.
   */
  public String getDriverStderrFileName() {
    return DRIVER_STDERR;
  }

  /**
   * @return The name used within the current working directory of the driver to redirect standard out to.
   */
  public String getDriverStdoutFileName() {
    return DRIVER_STDOUT;
  }

  /**
   * @return The prefix used whenever REEF is asked to create an Evaluator folder, e.g. for staging.
   */
  public String getEvaluatorFolderPrefix() {
    return EVALUATOR_FOLDER_PREFIX;
  }

  /**
   * @return The name used within the current working directory of the driver to redirect standard error to.
   */
  public String getEvaluatorStderrFileName() {
    return EVALUATOR_STDERR;
  }

  /**
   * @return The name used within the current working directory of the driver to redirect standard out to.
   */
  public String getEvaluatorStdoutFileName() {
    return EVALUATOR_STDOUT;
  }

  /**
   * @return File name that contains the dfs path for the DriverHttpEndpoint.
   */
  public String getDriverHttpEndpoint() {
    return DRIVER_HTTP_ENDPOINT_FILE_NAME;
  }

  /**
   * @return File name that contains the security token identifier
   */
  public String getSecurityTokenIdentifierFile() {
    return SECURITY_TOKEN_IDENTIFIER_FILE;
  }

  /**
   * @return File name that contains the security token password
   */
  public String getSecurityTokenPasswordFile() {
    return SECURITY_TOKEN_PASSWORD_FILE;
  }

  /**
   * @return File name the contains the bootstrap application parameters for YARN job submission
   * without Java dependency.
   */
  public String getYarnBootstrapAppParamFile() {
    return YARN_BOOTSTRAP_APP_PARAM_FILE;
  }

  /**
   * @return File name the contains the bootstrap job parameters for YARN job submission
   * without Java dependency.
   */
  public String getYarnBootstrapJobParamFile() {
    return YARN_BOOTSTRAP_JOB_PARAM_FILE;
  }

  /**
   * @return Path to the bootstrap application parameters file for YARN job submission without Java dependency.
   */
  public String getYarnBootstrapAppParamFilePath() {
    return LOCAL_FOLDER_PATH + '/' + getYarnBootstrapAppParamFile();
  }

  /**
   * @return Path to the bootstrap job parameters file for YARN job submission without Java dependency.
   */
  public String getYarnBootstrapJobParamFilePath() {
    return getYarnBootstrapJobParamFile();
  }
}
