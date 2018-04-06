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
package org.apache.reef.bridge.client.events;

import org.apache.reef.annotations.audience.Private;
import org.apache.reef.bridge.client.IDriverServiceClient;
import org.apache.reef.bridge.client.JVMClientProcess;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorDescriptor;
import org.apache.reef.driver.evaluator.EvaluatorProcess;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.util.Optional;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Allocated Evaluator Stub.
 */
@Private
public final class AllocatedEvaluatorStub implements AllocatedEvaluator {

  private final String evaluatorId;

  private final EvaluatorDescriptor evaluatorDescriptor;

  private final IDriverServiceClient driverServiceClient;

  private final ConfigurationSerializer configurationSerializer;

  private final List<File> addFileList = new ArrayList<>();

  private final List<File> addLibraryList = new ArrayList<>();

  private JVMClientProcess evaluatorProcess = null;

  public AllocatedEvaluatorStub(
      final String evaluatorId,
      final EvaluatorDescriptor evaluatorDescriptor,
      final IDriverServiceClient driverServiceClient,
      final ConfigurationSerializer configurationSerializer) {
    this.evaluatorId = evaluatorId;
    this.evaluatorDescriptor = evaluatorDescriptor;
    this.driverServiceClient = driverServiceClient;
    this.configurationSerializer = configurationSerializer;
  }

  @Override
  public String getId() {
    return this.evaluatorId;
  }

  @Override
  public void addFile(final File file) {
    this.addFileList.add(file);
  }

  @Override
  public void addLibrary(final File file) {
    this.addLibraryList.add(file);
  }

  @Override
  public EvaluatorDescriptor getEvaluatorDescriptor() {
    return this.evaluatorDescriptor;
  }

  @Override
  public void setProcess(final EvaluatorProcess process) {
    if (process instanceof JVMClientProcess) {
      this.evaluatorProcess = (JVMClientProcess) process;
    } else {
      throw new IllegalArgumentException(JVMClientProcess.class.getCanonicalName() + " required.");
    }
  }

  @Override
  public void close() {
    this.driverServiceClient.onEvaluatorClose(getId());
  }

  @Override
  public void submitTask(final Configuration taskConfiguration) {
    this.driverServiceClient.onEvaluatorSubmit(
        getId(),
        Optional.<Configuration>empty(),
        Optional.of(taskConfiguration),
        this.evaluatorProcess== null ?
            Optional.<JVMClientProcess>empty() :
            Optional.of(this.evaluatorProcess),
        this.addFileList.size() == 0 ?
            Optional.<List<File>>empty() :
            Optional.of(this.addFileList),
        this.addLibraryList.size() == 0 ?
            Optional.<List<File>>empty() :
            Optional.of(this.addLibraryList));
  }

  @Override
  public void submitContext(final Configuration contextConfiguration) {

    this.driverServiceClient.onEvaluatorSubmit(
        getId(),
        Optional.of(contextConfiguration),
        Optional.<Configuration>empty(),
        this.evaluatorProcess== null ?
            Optional.<JVMClientProcess>empty() :
            Optional.of(this.evaluatorProcess),
        this.addFileList.size() == 0 ?
            Optional.<List<File>>empty() :
            Optional.of(this.addFileList),
        this.addLibraryList.size() == 0 ?
            Optional.<List<File>>empty() :
            Optional.of(this.addLibraryList));
  }

  @Override
  public void submitContextAndService(
      final Configuration contextConfiguration,
      final Configuration serviceConfiguration) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void submitContextAndTask(
      final Configuration contextConfiguration,
      final Configuration taskConfiguration) {

    this.driverServiceClient.onEvaluatorSubmit(
        getId(),
        Optional.of(contextConfiguration),
        Optional.of(taskConfiguration),
        this.evaluatorProcess== null ?
            Optional.<JVMClientProcess>empty() :
            Optional.of(this.evaluatorProcess),
        this.addFileList.size() == 0 ?
            Optional.<List<File>>empty() :
            Optional.of(this.addFileList),
        this.addLibraryList.size() == 0 ?
            Optional.<List<File>>empty() :
            Optional.of(this.addLibraryList));
  }

  @Override
  public void submitContextAndServiceAndTask(
      final Configuration contextConfiguration,
      final Configuration serviceConfiguration,
      final Configuration taskConfiguration) {
    throw new UnsupportedOperationException();
  }


}
