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
package org.apache.reef.runtime.hdinsight;

import net.jcip.annotations.Immutable;
import org.apache.reef.runtime.common.files.RuntimeClasspathProvider;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Access to the classpath according to the REEF file system standard.
 */
@Immutable
public final class HDInsightClasspathProvider implements RuntimeClasspathProvider {

  private static final List<String> CLASSPATH_PREFIX = Collections
      .unmodifiableList(Arrays.asList("%HADOOP_HOME%/etc/hadoop"));

  private static final List<String> CLASSPATH_SUFFIX = Collections.unmodifiableList(
      Arrays.asList(
          "%HADOOP_HOME%/share/hadoop/common/*",
          "%HADOOP_HOME%/share/hadoop/common/lib/*",
          "%HADOOP_HOME%/share/hadoop/yarn/*",
          "%HADOOP_HOME%/share/hadoop/yarn/lib/*",
          "%HADOOP_HOME%/share/hadoop/hdfs/*",
          "%HADOOP_HOME%/share/hadoop/hdfs/lib/*",
          "%HADOOP_HOME%/share/hadoop/mapreduce/*",
          "%HADOOP_HOME%/share/hadoop/mapreduce/lib/*")
  );

  @Inject
  HDInsightClasspathProvider() {
  }

  @Override
  public List<String> getDriverClasspathPrefix() {
    return CLASSPATH_PREFIX;
  }

  @Override
  public List<String> getDriverClasspathSuffix() {
    return CLASSPATH_SUFFIX;
  }

  @Override
  public List<String> getEvaluatorClasspathPrefix() {
    return CLASSPATH_PREFIX;
  }

  @Override
  public List<String> getEvaluatorClasspathSuffix() {
    return CLASSPATH_SUFFIX;
  }
}
