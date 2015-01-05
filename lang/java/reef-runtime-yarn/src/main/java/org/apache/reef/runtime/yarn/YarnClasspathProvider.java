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
package org.apache.reef.runtime.yarn;

import net.jcip.annotations.Immutable;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.reef.runtime.common.files.RuntimeClasspathProvider;
import org.apache.reef.util.OSUtils;

import javax.inject.Inject;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Access to the classpath according to the REEF file system standard.
 */
@Immutable
public final class YarnClasspathProvider implements RuntimeClasspathProvider {
  private static final Logger LOG = Logger.getLogger(YarnClasspathProvider.class.getName());
  private static final Level CLASSPATH_LOG_LEVEL = Level.FINE;

  private static final String YARN_TOO_OLD_MESSAGE = "The version of YARN you are using is too old to support classpath assembly. Reverting to legacy method.";
  private static final String HADOOP_CONF_DIR = OSUtils.formatVariable("HADOOP_CONF_DIR");
  private static final String HADOOP_HOME = OSUtils.formatVariable("HADOOP_HOME");
  private static final String HADOOP_COMMON_HOME = OSUtils.formatVariable("HADOOP_COMMON_HOME");
  private static final String HADOOP_YARN_HOME = OSUtils.formatVariable("HADOOP_YARN_HOME");
  private static final String HADOOP_HDFS_HOME = OSUtils.formatVariable("HADOOP_HDFS_HOME");
  private static final String HADOOP_MAPRED_HOME = OSUtils.formatVariable("HADOOP_MAPRED_HOME");

  // Used when we can't get a classpath from YARN
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
  YarnClasspathProvider(final YarnConfiguration yarnConfiguration) {
    boolean needsLegacyClasspath = false; // will be set to true below whenever we encounter issues with the YARN Configuration
    final ClassPathBuilder builder = new ClassPathBuilder();

    try {
      // Add the classpath actually configured on this cluster
      final String[] yarnClassPath = yarnConfiguration.getTrimmedStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH);
      if (null == yarnClassPath || yarnClassPath.length == 0) {
        needsLegacyClasspath = true;
      } else {
        builder.addAll(yarnClassPath);
      }
      final String[] yarnDefaultClassPath = YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH;
      if (null == yarnDefaultClassPath || yarnDefaultClassPath.length == 0) {
        LOG.log(Level.SEVERE, "YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH is empty. This indicates a broken cluster configuration");
        needsLegacyClasspath = true;
      } else {
        builder.addAll(yarnDefaultClassPath);
      }
    } catch (final NoSuchFieldError e) {
      // This means that one of the static fields above aren't actually in YarnConfiguration.
      // The reason for that is most likely that we encounter a really old version of YARN.
      needsLegacyClasspath = true;
      LOG.log(Level.SEVERE, YARN_TOO_OLD_MESSAGE);
    }

    if (needsLegacyClasspath) {
      builder.addAll(LEGACY_CLASSPATH_LIST);
    }

    this.classPathPrefix = builder.getPrefixAsImmutableList();
    this.classPathSuffix = builder.getSuffixAsImmutableList();
    this.logClasspath();
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


  private void logClasspath() {
    if (LOG.isLoggable(CLASSPATH_LOG_LEVEL)) {
      final StringBuilder message = new StringBuilder("Classpath:\n\t");
      message.append(StringUtils.join(classPathPrefix, "\n\t"));
      message.append("\n--------------------------------\n\t");
      message.append(StringUtils.join(classPathSuffix, "\n\t"));
      LOG.log(CLASSPATH_LOG_LEVEL, message.toString());
    }
  }
}
