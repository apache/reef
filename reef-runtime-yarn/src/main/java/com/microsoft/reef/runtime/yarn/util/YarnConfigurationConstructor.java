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
package com.microsoft.reef.runtime.yarn.util;

import com.microsoft.reef.runtime.yarn.driver.YarnMasterConfiguration;
import com.microsoft.reef.util.Optional;
import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.annotations.Parameter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import javax.inject.Inject;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An external constructor that creates YarnConfiguration instances.
 */
public final class YarnConfigurationConstructor implements ExternalConstructor<YarnConfiguration> {
  private static final Logger LOG = Logger.getLogger(YarnConfigurationConstructor.class.getName());

  private final Optional<Path> configurationPath;

  @Inject
  YarnConfigurationConstructor() {
    this(Optional.<Path>empty());
  }

  @Inject
  YarnConfigurationConstructor(@Parameter(YarnMasterConfiguration.YarnConfigurationFile.class) final String file) {
    this(Optional.of(new Path(file)));
  }

  private YarnConfigurationConstructor(final Optional<Path> configurationFilePath) {
    this.configurationPath = configurationFilePath;
    LOG.log(Level.INFO, "Instantiated 'YarnConfigurationConstructor' with path {0}", configurationFilePath);
  }


  @Override
  public YarnConfiguration newInstance() {
    final YarnConfiguration yarnConfiguration = new YarnConfiguration();
    if (this.configurationPath.isPresent()) {
      yarnConfiguration.addResource(this.configurationPath.get());
    }
    // TODO: This should not be needed.
    yarnConfiguration.reloadConfiguration();

    LOG.log(Level.INFO, "Instantiated 'YarnConfiguration' with path [{0}] and contents [{1}] ",
        new Object[]{this.configurationPath, yarnConfiguration.toString()});
    return yarnConfiguration;
  }

  @Override
  public String toString() {
    return "YarnConfigurationConstructor{configurationPath=" + this.configurationPath + '}';
  }

  /**
   * @return A Configuration primed with the contents of $HADOOP_HOME/etc/hadoop/*.xml   *
   */
  // TODO: This is a hack. Just calling new Configuration(true) should do this, but did not on HDInsight.
  private static Configuration getDefaultConfiguration() {
    final Configuration conf = new Configuration(false);
    final File hadoopConfigurationFolder = new File(System.getenv("HADOOP_HOME") + "/etc/hadoop/");
    final List<File> configurationFiles = new ArrayList<>();
    for (final File f : hadoopConfigurationFolder.listFiles()) {
      if (f.isFile() && f.getName().endsWith(".xml")) {
        conf.addResource(new Path(f.getAbsolutePath()));
      }
    }
    return conf;
  }
}
