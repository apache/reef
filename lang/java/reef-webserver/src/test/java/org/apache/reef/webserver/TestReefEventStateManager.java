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
package org.apache.reef.webserver;

import org.apache.reef.driver.catalog.NodeDescriptor;
import org.apache.reef.driver.catalog.RackDescriptor;
import org.apache.reef.driver.evaluator.CLRProcessFactory;
import org.apache.reef.driver.evaluator.EvaluatorDescriptor;
import org.apache.reef.driver.evaluator.EvaluatorProcess;
import org.apache.reef.driver.evaluator.EvaluatorProcessFactory;
import org.apache.reef.driver.evaluator.SchedulingConstraint;
import org.apache.reef.runtime.common.driver.parameters.JobIdentifier;
import org.apache.reef.runtime.common.launch.REEFMessageCodec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.remote.RemoteConfiguration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.Map;

/**
 * Test ReefEventStateManager.
 */
public class TestReefEventStateManager {
  private Injector injector;
  private ReefEventStateManager reefEventStateManager;

  @Before
  public void setUp() throws InjectionException {
    final Tang tang = Tang.Factory.getTang();

    final Configuration configuration = tang.newConfigurationBuilder()
        .bindImplementation(EvaluatorDescriptor.class, MockEvaluatorDescriptor.class)
        .bindImplementation(NodeDescriptor.class, MockNodeDescriptor.class)
        .bindImplementation(EvaluatorProcessFactory.class, CLRProcessFactory.class)
        .bindNamedParameter(RemoteConfiguration.ManagerName.class, "REEF_TEST_REMOTE_MANAGER")
        .bindNamedParameter(RemoteConfiguration.MessageCodec.class, REEFMessageCodec.class)
        .bindNamedParameter(JobIdentifier.class, "my job")
        .build();

    injector = tang.newInjector(configuration);
    reefEventStateManager = injector.getInstance(ReefEventStateManager.class);
  }

  @Test
  public void reefEventStateManagerTest() throws InjectionException {
    Assert.assertNotNull(reefEventStateManager);
  }

  @Test
  public void addEvaluatorTest() throws InjectionException {
    final EvaluatorDescriptor evaluatorDescriptor = injector.getInstance(MockEvaluatorDescriptor.class);
    reefEventStateManager.put("1234", evaluatorDescriptor);
    final Map<String, EvaluatorDescriptor> evaluators = reefEventStateManager.getEvaluators();
    Assert.assertEquals(1, evaluators.size());
  }

  @Test
  public void getEvaluatorNodeDescriptorTest() throws InjectionException {
    final EvaluatorDescriptor evaluatorDescriptor = injector.getInstance(MockEvaluatorDescriptor.class);
    reefEventStateManager.put("1234", evaluatorDescriptor);
    final NodeDescriptor nodeDescriptor = reefEventStateManager.getEvaluatorNodeDescriptor("1234");
    Assert.assertEquals("myKey", nodeDescriptor.getId());
  }

  @Test
  public void getEvaluatorDescriptorTest() throws InjectionException {
    final EvaluatorDescriptor evaluatorDescriptor = injector.getInstance(MockEvaluatorDescriptor.class);
    reefEventStateManager.put("1234", evaluatorDescriptor);
    final EvaluatorDescriptor evaluatorDescriptor1 = reefEventStateManager.getEvaluatorDescriptor("1234");
    Assert.assertEquals(evaluatorDescriptor, evaluatorDescriptor1);
  }
}

final class MockEvaluatorDescriptor implements EvaluatorDescriptor {
  private final NodeDescriptor nodeDescriptor;
  private final EvaluatorProcessFactory evaluatorProcessFactory;

  @Inject
  MockEvaluatorDescriptor(final NodeDescriptor nodeDescriptor,
                          final EvaluatorProcessFactory evaluatorProcessFactory) {
    this.nodeDescriptor = nodeDescriptor;
    this.evaluatorProcessFactory = evaluatorProcessFactory;
  }

  @Override
  public NodeDescriptor getNodeDescriptor() {
    return nodeDescriptor;
  }

  @Override
  public EvaluatorProcess getProcess() {
    return evaluatorProcessFactory.newEvaluatorProcess();
  }

  @Override
  public int getMemory() {
    return 64;
  }

  @Override
  public int getNumberOfCores() {
    return 1;
  }

  @Override
  public String getRuntimeName() {
    return "Local";
  }

  @Override
  public SchedulingConstraint getSchedulingConstraint() {
    return null;
  }
}

final class MockNodeDescriptor implements NodeDescriptor {
  @Inject
  MockNodeDescriptor() {
  }

  @Override
  public InetSocketAddress getInetSocketAddress() {
    return null;
  }

  @Override
  public RackDescriptor getRackDescriptor() {
    return null;
  }

  @Override
  public String getName() {
    return "myName";
  }

  @Override
  public String getId() {
    return "myKey";
  }
}
