/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.runtime.common.files;

import com.microsoft.reef.util.OSUtils;
import net.jcip.annotations.Immutable;
import org.apache.commons.lang.StringUtils;

import javax.inject.Inject;
import java.io.File;
import java.util.Arrays;
import java.util.List;

/**
 * Access to the various places things go according to the REEF file system standard.
 */
@Immutable
public final class REEFFileNames {

  private static final String REEF_BASE_FOLDER = "reef";
  private static final String GLOBAL_FOLDER = "global";
  private static final String LOCAL_FOLDER = "local";
  private static final String DRIVER_CONFIGURATION_NAME = "driver.conf";
  private static final String EVALUATOR_CONFIGURATION_NAME = "evaluator.conf";
  private static final String JAR_FILE_SUFFIX = ".jar";
  private static final String JOB_FOLDER_PREFIX = "reef-job-";
  private static final String EVALUATOR_FOLDER_PREFIX = "reef-evaluator-";
  private static final String DRIVER_STDERR = "driver.stderr";
  private static final String DRIVER_STDOUT = "driver.stdout";
  private static final String EVALUATOR_STDERR = "evaluator.stderr";
  private static final String EVALUATOR_STDOUT = "evaluator.stdout";

  @Inject
  public REEFFileNames() {
  }

  /**
   * The name of the REEF folder inside of the working directory of an Evaluator or Driver
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
    return getREEFFolderName() + "/" + getGlobalFolderName();
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
    return getREEFFolderName() + "/" + getLocalFolderName();
  }

  /**
   * @return the folder holding the files local to this container.
   */
  public File getLocalFolder() {
    return new File(getREEFFolder(), getLocalFolderName());
  }


  /**
   * The name under which the driver configuration will be stored in REEF_BASE_FOLDER/LOCAL_FOLDER
   */
  public String getDriverConfigurationName() {
    return DRIVER_CONFIGURATION_NAME;
  }

  /**
   * @return The path to the driver configuration: GLOBAL_FOLDER/LOCAL_FOLDER/DRIVER_CONFIGURATION_NAME
   */
  public String getDriverConfigurationPath() {
    return getLocalFolderPath() + "/" + DRIVER_CONFIGURATION_NAME;
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
    return getLocalFolderPath() + "/" + getEvaluatorConfigurationName();
  }

  /**
   * @return The suffix used for JAR files, including the "."
   */
  public String getJarFileSuffix() {
    return JAR_FILE_SUFFIX;
  }

  /**
   * The prefix used whenever REEF is asked to create a job folder, on (H)DFS or locally.
   * <p/>
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

  private static String wrapVar(final String var) {
    return OSUtils.isWindows() ? "%" + var + "%" : "$" + var;
  }

  private final static String HADOOP_CONF_DIR = wrapVar("HADOOP_CONF_DIR");
  private final static String HADOOP_HOME = wrapVar("HADOOP_HOME");
  private final static String HADOOP_COMMON_HOME = wrapVar("HADOOP_COMMON_HOME");
  private final static String HADOOP_YARN_HOME = wrapVar("HADOOP_YARN_HOME");
  private final static String HADOOP_HDFS_HOME = wrapVar("HADOOP_HDFS_HOME");
  private final static String HADOOP_MAPRED_HOME = wrapVar("HADOOP_MAPRED_HOME");

  /**
   * @return the class path for the process.
   */
  public String getClasspath() {
    return StringUtils.join(getClasspathList(), File.pathSeparatorChar);
  }

  public List<String> getClasspathList() {
    return Arrays.asList(
        HADOOP_CONF_DIR,
        HADOOP_HOME + "/*",
        HADOOP_HOME + "/lib/*",
        HADOOP_COMMON_HOME + "/*",
        HADOOP_COMMON_HOME + "/lib/*",
        HADOOP_YARN_HOME + "/*",
        HADOOP_YARN_HOME + "/lib/*",
        HADOOP_HDFS_HOME + "/*",
        HADOOP_HDFS_HOME + "/lib/*",
        HADOOP_MAPRED_HOME + "/*",
        HADOOP_MAPRED_HOME + "/lib/*",
        HADOOP_HOME + "/etc/hadoop",
        HADOOP_HOME + "/share/hadoop/common/*",
        HADOOP_HOME + "/share/hadoop/common/lib/*",
        HADOOP_HOME + "/share/hadoop/yarn/*",
        HADOOP_HOME + "/share/hadoop/yarn/lib/*",
        HADOOP_HOME + "/share/hadoop/hdfs/*",
        HADOOP_HOME + "/share/hadoop/hdfs/lib/*",
        HADOOP_HOME + "/share/hadoop/mapreduce/*",
        HADOOP_HOME + "/share/hadoop/mapreduce/lib/*",
        getLocalFolderPath() + "/*",
        getGlobalFolderPath() + "/*");
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
}
