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
package org.apache.reef.runtime.spark;

import net.jcip.annotations.Immutable;
import org.apache.reef.runtime.common.files.RuntimeClasspathProvider;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.List;

/**
 * Access to the classpath according to the REEF file system standard.
 */
@Immutable
public final class SparkClasspathProvider implements RuntimeClasspathProvider {
  private static final String HADOOP_CONF_DIR = System.getenv("HADOOP_CONF_DIR");
  private static final String HADOOP_HOME = System.getenv("HADOOP_HOME");
  private static final String HADOOP_COMMON_HOME = System.getenv("HADOOP_COMMON_HOME");
  private static final String HADOOP_YARN_HOME = System.getenv("HADOOP_YARN_HOME");
  private static final String HADOOP_HDFS_HOME = System.getenv("HADOOP_HDFS_HOME");
  private static final String HADOOP_MAPRED_HOME = System.getenv("HADOOP_MAPRED_HOME");

  // Used when we can't get a classpath from Hadoop
  private static final String[] LEGACY_CLASSPATH_LIST = new String[]{
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
      HADOOP_HOME + "/share/hadoop/mapreduce/lib/*"
  };
  private final List<String> classPathPrefix;
  private final List<String> classPathSuffix;

  @Inject
  SparkClasspathProvider() {
    this.classPathPrefix = Arrays.asList(LEGACY_CLASSPATH_LIST);
    this.classPathSuffix = Arrays.asList(LEGACY_CLASSPATH_LIST);
  }

  @Override
  public List<String> getDriverClasspathPrefix() {
    return this.classPathPrefix;
  }

  @Override
  public List<String> getDriverClasspathSuffix() {
    return this.classPathSuffix;
  }

  @Override
  public List<String> getEvaluatorClasspathPrefix() {
    return this.classPathPrefix;
  }

  @Override
  public List<String> getEvaluatorClasspathSuffix() {
    return this.classPathSuffix;
  }
}
