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
package org.apache.reef.runtime.local.driver;

import org.apache.reef.runtime.common.driver.api.ResourceRequestEvent;
import org.apache.reef.runtime.common.driver.api.ResourceRequestEventImpl;
import org.apache.reef.runtime.common.driver.api.RuntimeParameters;
import org.apache.reef.runtime.common.driver.resourcemanager.NodeDescriptorEvent;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceAllocationEvent;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceStatusEvent;
import org.apache.reef.runtime.common.driver.resourcemanager.RuntimeStatusEvent;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.common.utils.RemoteManager;
import org.apache.reef.runtime.local.client.parameters.MaxNumberOfEvaluators;
import org.apache.reef.runtime.local.client.parameters.RackNames;
import org.apache.reef.runtime.local.client.parameters.RootFolder;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.util.logging.LoggingScopeFactory;
import org.apache.reef.wake.EventHandler;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.*;

/**
 * Unit test for Resource Manager (and ContainerManager) classes.
 *
 */
public class ResourceManagerTest {

  private Injector injector;

  private ResourceManager resourceManager;
  private RemoteManager remoteManager;
  private EventHandler<ResourceStatusEvent> mockRuntimeResourceStatusHandler;
  private EventHandler<NodeDescriptorEvent> mockNodeDescriptorHandler;
  private EventHandler<ResourceAllocationEvent> mockResourceAllocationHandler;
  private EventHandler<RuntimeStatusEvent> mockRuntimeStatusHandler;
  private REEFFileNames filenames;
  private ContainerManager containerManager;
  private ConfigurationSerializer configurationSerializer;
  private static final double JVM_HEAP_SLACK = 0.1;
  private LoggingScopeFactory loggingScopeFactory;

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() throws InjectionException {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindNamedParameter(RootFolder.class, "target/REEF_LOCAL_RUNTIME");
    injector = Tang.Factory.getTang().newInjector(cb.build());
    remoteManager = injector.getInstance(RemoteManager.class);
    mockRuntimeResourceStatusHandler = mock(EventHandler.class);
    injector.bindVolatileParameter(RuntimeParameters.ResourceStatusHandler.class, mockRuntimeResourceStatusHandler);
    mockNodeDescriptorHandler = mock(EventHandler.class);
    injector.bindVolatileParameter(RuntimeParameters.NodeDescriptorHandler.class, mockNodeDescriptorHandler);
    mockResourceAllocationHandler = mock(EventHandler.class);
    injector.bindVolatileParameter(RuntimeParameters.ResourceAllocationHandler.class, mockResourceAllocationHandler);
    mockRuntimeStatusHandler = mock(EventHandler.class);
    injector.bindVolatileParameter(RuntimeParameters.RuntimeStatusHandler.class, mockRuntimeStatusHandler);
    configurationSerializer = injector.getInstance(ConfigurationSerializer.class);
    filenames = injector.getInstance(REEFFileNames.class);
    loggingScopeFactory = injector.getInstance(LoggingScopeFactory.class);
  }

  @After
  public void tearDown() {
    // no need to reset mocks, they are created again in the setup
  }

  /**
   * Helper method to call the sendNodeDescriptors private method in the
   * containerManager, which populates the available containers in each rack.
   */
  private void sendNodeDescriptors() {
    try {
      final Method method = ContainerManager.class.getDeclaredMethod("sendNodeDescriptors");
      method.setAccessible(true);
      method.invoke(containerManager);
    } catch (final Exception exc) {
      throw new RuntimeException(exc);
    }
  }

  @Test(expected = InjectionException.class)
  public void testInvalidRacksConfigured() throws InjectionException {
    // Given
    final Set<String> availableRacks = new HashSet<>(Arrays.asList("/rack1/*"));
    injector.bindVolatileParameter(RackNames.class, availableRacks);
    // When
    containerManager = injector.getInstance(ContainerManager.class);
    // Then
    // expect the exception to be thrown
  }

  @Test
  public void testOneAllocationsInDefaultRack() throws InjectionException {
    // Given
    containerManager = injector.getInstance(ContainerManager.class);
    sendNodeDescriptors();
    resourceManager = new ResourceManager(containerManager, mockResourceAllocationHandler, mockRuntimeStatusHandler,
        JVM_HEAP_SLACK, configurationSerializer, remoteManager, filenames, loggingScopeFactory);
    final ResourceRequestEvent request = ResourceRequestEventImpl.newBuilder().setResourceCount(1).setVirtualCores(1)
        .setMemorySize(64).build();
    // When
    resourceManager.onResourceRequest(request);
    // Then
    verify(mockResourceAllocationHandler, times(1)).onNext(any(ResourceAllocationEvent.class));
    verify(mockRuntimeStatusHandler, times(2)).onNext(any(RuntimeStatusEvent.class));
  }

  @Test
  public void testZeroAllocationsDueToContainersNotAvailableAndRelaxLocalityDisabled() throws InjectionException {
    // Given
    containerManager = injector.getInstance(ContainerManager.class);
    // not sending notifications, there are no available free slots in the container manager
    resourceManager = new ResourceManager(containerManager, mockResourceAllocationHandler, mockRuntimeStatusHandler,
        JVM_HEAP_SLACK, configurationSerializer, remoteManager,
        filenames, loggingScopeFactory);
    final ResourceRequestEvent request = ResourceRequestEventImpl.newBuilder().setResourceCount(2).setVirtualCores(1)
        .setMemorySize(64).build();
    // When
    resourceManager.onResourceRequest(request);
    // Then
    verify(mockResourceAllocationHandler, times(0)).onNext(any(ResourceAllocationEvent.class));
    verify(mockRuntimeStatusHandler, times(1)).onNext(any(RuntimeStatusEvent.class));
  }

  @Test
  public void testTwoAllocationsInDifferentRacks() throws InjectionException {
    // Given
    final List<String> availableRacks = Arrays.asList("/rack1", "/rack2");
    final Set<String> availableRacksSet = new HashSet<>(availableRacks);
    injector.bindVolatileParameter(RackNames.class, availableRacksSet); // 2 available racks
    injector.bindVolatileParameter(MaxNumberOfEvaluators.class, 2); // 1 evaluator per rack
    containerManager = injector.getInstance(ContainerManager.class); // inject containerManager with this updated info
    sendNodeDescriptors();
    resourceManager = new ResourceManager(containerManager, mockResourceAllocationHandler, mockRuntimeStatusHandler,
        JVM_HEAP_SLACK, configurationSerializer, remoteManager,
        filenames, loggingScopeFactory);
    final ResourceRequestEvent request = ResourceRequestEventImpl.newBuilder().setResourceCount(2).setVirtualCores(1)
        .setMemorySize(64).addRackName(availableRacks.get(0)).addRackName(availableRacks.get(1)).build();
    // When
    resourceManager.onResourceRequest(request);
    // Then
    final ArgumentCaptor<ResourceAllocationEvent> argument = ArgumentCaptor.forClass(ResourceAllocationEvent.class);
    verify(mockResourceAllocationHandler, times(2)).onNext(argument.capture());
    final List<ResourceAllocationEvent> actualResourceAllocationEvent = argument.getAllValues();
    Assert.assertEquals("/rack1", actualResourceAllocationEvent.get(0).getRackName().get());
    Assert.assertEquals("/rack2", actualResourceAllocationEvent.get(1).getRackName().get());
    verify(mockRuntimeStatusHandler, times(3)).onNext(any(RuntimeStatusEvent.class));
  }

  @Test
  public void testTwoAllocationsOnFourContainersAvailableInDefaultRack() throws InjectionException {
    // Given
    containerManager = injector.getInstance(ContainerManager.class);
    sendNodeDescriptors();
    resourceManager = new ResourceManager(containerManager, mockResourceAllocationHandler, mockRuntimeStatusHandler,
        JVM_HEAP_SLACK, configurationSerializer, remoteManager,
        filenames, loggingScopeFactory);
    final ResourceRequestEvent request = ResourceRequestEventImpl.newBuilder().setResourceCount(2).setVirtualCores(1)
        .setMemorySize(64).build();
    // When
    resourceManager.onResourceRequest(request);
    // Then
    verify(mockResourceAllocationHandler, times(2)).onNext(any(ResourceAllocationEvent.class));
    verify(mockRuntimeStatusHandler, times(3)).onNext(any(RuntimeStatusEvent.class));
  }

  @Test
  public void testOneAllocationInRack1AndTwoInDatacenter2() throws InjectionException {
    // Given
    final List<String> availableRacks = Arrays.asList("/dc1/rack1", "/dc2/rack1", "/dc2/rack2");
    final Set<String> availableRacksSet = new HashSet<>(availableRacks);
    injector.bindVolatileParameter(RackNames.class, availableRacksSet); // 3 available racks
    injector.bindVolatileParameter(MaxNumberOfEvaluators.class, 3); // 1 evaluator per rack
    containerManager = injector.getInstance(ContainerManager.class);
    sendNodeDescriptors();
    resourceManager = new ResourceManager(containerManager, mockResourceAllocationHandler, mockRuntimeStatusHandler,
        JVM_HEAP_SLACK, configurationSerializer, remoteManager,
        filenames, loggingScopeFactory);
    final ResourceRequestEvent request = ResourceRequestEventImpl.newBuilder().setResourceCount(3).setVirtualCores(1)
        .setMemorySize(64).addRackName("dc1/*").addRackName("/dc2/*").build();
    // When
    resourceManager.onResourceRequest(request);
    // Then
    final ArgumentCaptor<ResourceAllocationEvent> argument = ArgumentCaptor.forClass(ResourceAllocationEvent.class);
    verify(mockResourceAllocationHandler, times(3)).onNext(argument.capture());
    final List<ResourceAllocationEvent> actualResourceAllocationEvent = argument.getAllValues();
    Assert.assertTrue(actualResourceAllocationEvent.get(0).getRackName().get().contains("/dc1"));
    Assert.assertTrue(actualResourceAllocationEvent.get(1).getRackName().get().contains("/dc2"));
    Assert.assertTrue(actualResourceAllocationEvent.get(2).getRackName().get().contains("/dc2"));
    verify(mockRuntimeStatusHandler, times(4)).onNext(any(RuntimeStatusEvent.class));
  }

  @Test
  public void testAllocateNode8AndTwoRandomOnesInDefaultRack() throws InjectionException {
    // Given
    injector.bindVolatileParameter(MaxNumberOfEvaluators.class, 8); // 8 evaluator in the default rack
    containerManager = injector.getInstance(ContainerManager.class);
    sendNodeDescriptors();
    resourceManager = new ResourceManager(containerManager, mockResourceAllocationHandler, mockRuntimeStatusHandler,
        JVM_HEAP_SLACK, configurationSerializer, remoteManager,
        filenames, loggingScopeFactory);
    final ResourceRequestEvent request = ResourceRequestEventImpl.newBuilder().setResourceCount(3).setVirtualCores(1)
        .setMemorySize(64).addNodeName("Node-8").build();
    // When
    resourceManager.onResourceRequest(request);
    // Then
    final ArgumentCaptor<ResourceAllocationEvent> argument = ArgumentCaptor.forClass(ResourceAllocationEvent.class);
    verify(mockResourceAllocationHandler, times(3)).onNext(argument.capture());
    final List<ResourceAllocationEvent> actualResourceAllocationEvent = argument.getAllValues();
    Assert.assertEquals("Node-8", actualResourceAllocationEvent.get(0).getNodeId());
    Assert.assertEquals(RackNames.DEFAULT_RACK_NAME, actualResourceAllocationEvent.get(0).getRackName().get());
    Assert.assertNotEquals("Node-8", actualResourceAllocationEvent.get(1).getNodeId());
    Assert.assertEquals(RackNames.DEFAULT_RACK_NAME, actualResourceAllocationEvent.get(1).getRackName().get());
    Assert.assertNotEquals("Node-8", actualResourceAllocationEvent.get(2).getNodeId());
    Assert.assertEquals(RackNames.DEFAULT_RACK_NAME, actualResourceAllocationEvent.get(2).getRackName().get());
    verify(mockRuntimeStatusHandler, times(4)).onNext(any(RuntimeStatusEvent.class));

  }

  @Test
  public void testOneAllocationInRack1AndTwoInDifferentRacksDueToRelaxLocality() throws InjectionException {
    // Given
    final List<String> availableRacks = Arrays.asList("/dc1/rack1", "/dc2/rack1", "/dc3/rack1");
    final Set<String> availableRacksSet = new HashSet<>(availableRacks);
    injector.bindVolatileParameter(RackNames.class, availableRacksSet);
    injector.bindVolatileParameter(MaxNumberOfEvaluators.class, 3); // 3 evaluators in three different racks
    containerManager = injector.getInstance(ContainerManager.class);
    sendNodeDescriptors();
    resourceManager = new ResourceManager(containerManager, mockResourceAllocationHandler, mockRuntimeStatusHandler,
        JVM_HEAP_SLACK, configurationSerializer, remoteManager,
        filenames, loggingScopeFactory);
    final ResourceRequestEvent request = ResourceRequestEventImpl.newBuilder().setResourceCount(3).setVirtualCores(1)
        .setMemorySize(64).addRackName("/dc3/rack1").addRackName("/*").build();
    // When
    resourceManager.onResourceRequest(request);
    // Then
    final ArgumentCaptor<ResourceAllocationEvent> argument = ArgumentCaptor.forClass(ResourceAllocationEvent.class);
    verify(mockResourceAllocationHandler, times(3)).onNext(argument.capture());
    final List<ResourceAllocationEvent> actualResourceAllocationEvent = argument.getAllValues();
    Assert.assertEquals("/dc3/rack1", actualResourceAllocationEvent.get(0).getRackName().get());
    Assert.assertNotEquals("/dc3/rack1", actualResourceAllocationEvent.get(1).getRackName().get());
    Assert.assertNotEquals("/dc3/rack1", actualResourceAllocationEvent.get(2).getRackName().get());
    verify(mockRuntimeStatusHandler, times(4)).onNext(any(RuntimeStatusEvent.class));

  }


}
