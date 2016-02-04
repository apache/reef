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
package org.apache.reef.runtime.common.launch;

import org.apache.commons.lang.StringUtils;
import org.apache.reef.util.Optional;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A builder for the command line to launch a CLR Evaluator.
 */
public class CLRLaunchCommandBuilder implements LaunchCommandBuilder {
  private static final Logger LOG = Logger.getLogger(CLRLaunchCommandBuilder.class.getName());
  private static final String EVALUATOR_PATH = "reef/global/Org.Apache.Reef.Evaluator.exe";

  private String standardErrPath = null;
  private String standardOutPath = null;
  private int megaBytes = 0;
  private Optional<List<String>> evaluatorConfigurationPaths = Optional.empty();

  @Override
  public List<String> build() {
    final List<String> result = new LinkedList<>();
    final File f = new File(EVALUATOR_PATH);
    if (!f.exists()) {
      LOG.log(Level.WARNING, "file can NOT be found: {0}", f.getAbsolutePath());
    }
    result.add(f.getPath());
    if (evaluatorConfigurationPaths.isPresent()) {
      for (final String evaluatorConfigurationPath : evaluatorConfigurationPaths.get()) {
        result.add(evaluatorConfigurationPath);
      }
    }
    if (null != this.standardOutPath && !standardOutPath.isEmpty()) {
      result.add(">" + this.standardOutPath);
    }
    if (null != this.standardErrPath && !standardErrPath.isEmpty()) {
      result.add("2>" + this.standardErrPath);
    }
    LOG.log(Level.FINE, "Launch Exe: {0}", StringUtils.join(result, ' '));
    return result;
  }

  @Override
  @SuppressWarnings("checkstyle:hiddenfield")
  public CLRLaunchCommandBuilder setMemory(final int megaBytes) {
    this.megaBytes = megaBytes;
    return this;
  }

  @Override
  public CLRLaunchCommandBuilder setConfigurationFilePaths(final List<String> configurationFilePaths) {
    this.evaluatorConfigurationPaths = Optional.of(configurationFilePaths);
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
