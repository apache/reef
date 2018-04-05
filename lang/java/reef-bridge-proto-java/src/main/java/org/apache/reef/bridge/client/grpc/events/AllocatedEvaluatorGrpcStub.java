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
package org.apache.reef.bridge.client.grpc.events;

import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorDescriptor;
import org.apache.reef.driver.evaluator.EvaluatorProcess;
import org.apache.reef.tang.Configuration;

import java.io.File;

/**
 * Allocated Evaluator Stub.
 */
public final class AllocatedEvaluatorGrpcStub implements AllocatedEvaluator {

  @Override
  public void addFile(final File file) {

  }

  @Override
  public void addLibrary(final File file) {

  }

  @Override
  public EvaluatorDescriptor getEvaluatorDescriptor() {
    return null;
  }

  @Override
  public void setProcess(final EvaluatorProcess process) {

  }

  @Override
  public void close() {

  }

  @Override
  public void submitTask(final Configuration taskConfiguration) {

  }

  @Override
  public void submitContext(final Configuration contextConfiguration) {

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

  }

  @Override
  public void submitContextAndServiceAndTask(
      final Configuration contextConfiguration,
      final Configuration serviceConfiguration,
      final Configuration taskConfiguration) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getId() {
    return null;
  }
}
