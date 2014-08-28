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
package com.microsoft.reef.util;

/**
 * Constants for the various Hadoop environment variables.
 */
public class HadoopEnvironment {

  public static final String HADOOP_CONF_DIR = "HADOOP_CONF_DIR";
  public static final String HADOOP_HOME = "HADOOP_HOME";
  public static final String HADOOP_COMMON_HOME = "HADOOP_COMMON_HOME";
  public static final String HADOOP_YARN_HOME = "HADOOP_YARN_HOME";
  public static final String HADOOP_HDFS_HOME = "HADOOP_HDFS_HOME";
  public static final String HADOOP_MAPRED_HOME = "HADOOP_MAPRED_HOME";

  private HadoopEnvironment() {
  }


}
