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
package com.microsoft.reef.runtime.yarn;

import com.microsoft.reef.runtime.common.files.RuntimeClasspathProvider;
import com.microsoft.reef.util.HadoopEnvironment;
import net.jcip.annotations.Immutable;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Access to the classpath according to the REEF file system standard.
 */
@Immutable
public final class YarnClasspathProvider implements RuntimeClasspathProvider {
  private static final Logger LOG = Logger.getLogger(YarnClasspathProvider.class.getName());

  private final List<String> classPathPrefix;
  private final List<String> classPathSuffix;


  @Inject
  YarnClasspathProvider(final YarnConfiguration yarnConfiguration) {
    final List<String> prefix = new ArrayList<>();
    final List<String> suffix = new ArrayList<>();
    for (final String classPathEntry : yarnConfiguration.getTrimmedStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH)) {
      // Make sure that the cluster configuration is in front of user classes
      if (classPathEntry.endsWith("conf") || classPathEntry.contains(HadoopEnvironment.HADOOP_CONF_DIR)) {
        prefix.add(classPathEntry);
      } else {
        suffix.add(classPathEntry);
      }
    }
    this.classPathPrefix = Collections.unmodifiableList(prefix);
    this.classPathSuffix = Collections.unmodifiableList(suffix);
    final StringBuilder message = new StringBuilder("Classpath:\n\t");
    message.append(StringUtils.join(classPathPrefix, "\n\t"));
    message.append("\n--------------------------------\n\t");
    message.append(StringUtils.join(classPathSuffix, "\n\t"));
    LOG.log(Level.FINEST, message.toString());
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
    return this.getDriverClasspathSuffix();
  }
}
