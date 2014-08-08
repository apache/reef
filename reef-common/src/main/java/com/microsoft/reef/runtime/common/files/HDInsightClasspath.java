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

import net.jcip.annotations.Immutable;
import org.apache.commons.lang.StringUtils;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.List;

/**
 * Access to the classpath according to the REEF file system standard.
 */
@Immutable
public final class HDInsightClasspath implements REEFClasspath {

  private static final List<String> CLASSPATH_LIST = Arrays.asList(
      "%HADOOP_HOME%/etc/hadoop",
      "%HADOOP_HOME%/share/hadoop/common/*",
      "%HADOOP_HOME%/share/hadoop/common/lib/*",
      "%HADOOP_HOME%/share/hadoop/yarn/*",
      "%HADOOP_HOME%/share/hadoop/yarn/lib/*",
      "%HADOOP_HOME%/share/hadoop/hdfs/*",
      "%HADOOP_HOME%/share/hadoop/hdfs/lib/*",
      "%HADOOP_HOME%/share/hadoop/mapreduce/*",
      "%HADOOP_HOME%/share/hadoop/mapreduce/lib/*",
      REEFFileNames.LOCAL_FOLDER_PATH + "/*",
      REEFFileNames.GLOBAL_FOLDER_PATH + "/*");

  private static final String CLASSPATH = StringUtils.join(CLASSPATH_LIST, ";");

  @Inject
  public HDInsightClasspath() {
  }

  /**
   * @return the class path for the process.
   */
  @Override
  public String getClasspath() {
    return CLASSPATH;
  }

  @Override
  public List<String> getClasspathList() {
    return CLASSPATH_LIST;
  }
}
