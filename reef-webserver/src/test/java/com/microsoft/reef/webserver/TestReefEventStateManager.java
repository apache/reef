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

package com.microsoft.reef.webserver;

import com.microsoft.reef.driver.catalog.NodeDescriptor;
import com.microsoft.reef.driver.catalog.RackDescriptor;
import com.microsoft.reef.driver.evaluator.EvaluatorDescriptor;
import com.microsoft.reef.driver.evaluator.EvaluatorType;
import com.microsoft.reef.runtime.common.driver.api.AbstractDriverRuntimeConfiguration;
import com.microsoft.reef.runtime.common.launch.REEFMessageCodec;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.wake.remote.RemoteConfiguration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.Map;

/**
 * Test ReefEventStateManager
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
        .bindNamedParameter(RemoteConfiguration.ManagerName.class, "REEF_TEST_REMOTE_MANAGER")
        .bindNamedParameter(RemoteConfiguration.MessageCodec.class, REEFMessageCodec.class)
        .bindNamedParameter(AbstractDriverRuntimeConfiguration.JobIdentifier.class, "my job")
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
  final private NodeDescriptor nodeDescriptor;

  @Inject
  public MockEvaluatorDescriptor(final NodeDescriptor nodeDescriptor) {
    this.nodeDescriptor = nodeDescriptor;
  }

  @Override
  public NodeDescriptor getNodeDescriptor() {
    return nodeDescriptor;
  }

  @Override
  public EvaluatorType getType() {
    return EvaluatorType.CLR;
  }

  @Override
  public int getMemory() {
    return 64;
  }

  @Override
  public int getNumberOfCores() {
    return 1;
  }
}

final class MockNodeDescriptor implements NodeDescriptor {
  @Inject
  public MockNodeDescriptor() {
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
