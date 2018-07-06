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

package org.apache.reef.bridge.driver.client;

import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.evaluator.EvaluatorProcess;
import org.apache.reef.driver.evaluator.EvaluatorType;

import java.util.ArrayList;
import java.util.List;

/**
 * Stub class for Client Process.
 */
@Private
public final class JVMClientProcess implements EvaluatorProcess {

  private boolean optionSet = false;

  private int megaBytes = 0;

  private String configurationFileName = null;

  private String standardOut = null;

  private String standardErr = null;

  private final List<String> optionList = new ArrayList<>();

  public JVMClientProcess() {
  }

  @Override
  public List<String> getCommandLine() {
    throw new UnsupportedOperationException("Getting command line on the client is not supported.");
  }

  @Override
  public EvaluatorType getType() {
    return EvaluatorType.JVM;
  }

  @Override
  public JVMClientProcess setMemory(final int mb) {
    this.megaBytes = mb;
    this.optionSet = true;
    return this;
  }

  public int getMemory() {
    return this.megaBytes;
  }

  @Override
  public boolean isOptionSet() {
    return optionSet;
  }

  @Override
  public JVMClientProcess setConfigurationFileName(final String configurationFileName) {
    this.configurationFileName = configurationFileName;
    return this;
  }

  public String getConfigurationFileName() {
    return this.configurationFileName;
  }

  @Override
  public JVMClientProcess setStandardOut(final String standardOut) {
    this.standardOut = standardOut;
    return this;
  }

  public String getStandardOut() {
    return this.standardOut;
  }

  @Override
  public JVMClientProcess setStandardErr(final String standardErr) {
    this.standardErr = standardErr;
    return this;
  }

  public String getStandardErr() {
    return this.standardErr;
  }

  /**
   * Add a JVM option.
   * @param option The full option, e.g. "-XX:+PrintGCDetails", "-Xms500m"
   * @return this
   */
  public JVMClientProcess addOption(final String option) {
    this.optionList.add(option);
    optionSet = true;
    return this;
  }

  public List<String> getOptions() {
    return this.optionList;
  }
}
