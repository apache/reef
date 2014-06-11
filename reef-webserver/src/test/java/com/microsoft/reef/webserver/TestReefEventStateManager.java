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
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.InjectionException;
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
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindImplementation(EvaluatorDescriptor.class, MockEvaluatorDescriptor.class);
    cb.bindImplementation(NodeDescriptor.class, MockNodeDescriptor.class);
    final Configuration configuration = cb.build();
    injector = Tang.Factory.getTang().newInjector(configuration);
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
  private NodeDescriptor nodeDescriptor;

  @Inject
  public MockEvaluatorDescriptor(NodeDescriptor nodeDescriptor) {
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
