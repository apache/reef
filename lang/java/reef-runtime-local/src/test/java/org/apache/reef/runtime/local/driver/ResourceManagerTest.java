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
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.util.logging.LoggingScopeFactory;
import org.apache.reef.wake.EventHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;

public class ResourceManagerTest {

  Injector injector;

  private ResourceManager resourceManager;
  private RemoteManager remoteManager;
  private EventHandler<ResourceStatusEvent> mockRuntimeResourceStatusHandler;
  private EventHandler<NodeDescriptorEvent> mockNodeDescriptorHandler;
  private EventHandler<ResourceAllocationEvent> mockResourceAllocationHandler;
  private EventHandler<RuntimeStatusEvent> mockRuntimeStatusHandler;
  private REEFFileNames filenames;
  private ContainerManager containerManager;
  private ConfigurationSerializer configurationSerializer;
  private static final int DEFAULT_CORES = 2;
  private static final int DEFAULT_MEMORY_SIZE = 512;
  private static final double JVM_HEAP_SLACK = 0.1;
  private LoggingScopeFactory loggingScopeFactory;

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() throws InjectionException {
    injector = Tang.Factory.getTang().newInjector();
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
   * containerManager, which populates the available containers in each rack
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

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidRackPassedInTheRequest() throws InjectionException {
    // Given
    containerManager = injector.getInstance(ContainerManager.class);
    final Set<String> availableRacks = new HashSet<String>(Arrays.asList("rack1"));
    resourceManager = new ResourceManager(containerManager, mockResourceAllocationHandler, mockRuntimeStatusHandler,
        DEFAULT_MEMORY_SIZE, DEFAULT_CORES, availableRacks, JVM_HEAP_SLACK, configurationSerializer, remoteManager,
        filenames, loggingScopeFactory);
    final ResourceRequestEvent request = ResourceRequestEventImpl.newBuilder().setResourceCount(2).setVirtualCores(1)
        .setMemorySize(64).build();
    // When
    resourceManager.onResourceRequest(request);
    // Then
    // expect the exception to be thrown
  }

  @Test
  public void testZeroAllocationsDueToContainersNotAvailableAndRelaxLocalityEnabled() throws InjectionException {
    // Given
    containerManager = injector.getInstance(ContainerManager.class);
    final Set<String> availableRacks = new HashSet<String>(Arrays.asList(RackNames.DEFAULT_RACK_NAME));
    resourceManager = new ResourceManager(containerManager, mockResourceAllocationHandler, mockRuntimeStatusHandler,
        DEFAULT_MEMORY_SIZE, DEFAULT_CORES, availableRacks, JVM_HEAP_SLACK, configurationSerializer, remoteManager,
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
  public void testZeroAllocationsDueToContainersNotAvailableAndRelaxLocalityDisabled() throws InjectionException {
    // Given
    containerManager = injector.getInstance(ContainerManager.class);
    final Set<String> availableRacks = new HashSet<String>(Arrays.asList(RackNames.DEFAULT_RACK_NAME));
    resourceManager = new ResourceManager(containerManager, mockResourceAllocationHandler, mockRuntimeStatusHandler,
        DEFAULT_MEMORY_SIZE, DEFAULT_CORES, availableRacks, JVM_HEAP_SLACK, configurationSerializer, remoteManager,
        filenames, loggingScopeFactory);
    final ResourceRequestEvent request = ResourceRequestEventImpl.newBuilder().setResourceCount(2).setVirtualCores(1)
        .setMemorySize(64).setRelaxLocality(Boolean.FALSE).build();
    // When
    resourceManager.onResourceRequest(request);
    // Then
    verify(mockResourceAllocationHandler, times(0)).onNext(any(ResourceAllocationEvent.class));
    verify(mockRuntimeStatusHandler, times(1)).onNext(any(RuntimeStatusEvent.class));
  }

  @Test
  public void testTwoAllocationsInDifferentRacksDueToRelaxLocalityEnabled() throws InjectionException {
    // Given
    final List<String> availableRacks = Arrays.asList("rack1", "rack2");
    final Set<String> availableRacksSet = new HashSet<String>(availableRacks);
    injector.bindVolatileParameter(RackNames.class, availableRacksSet); // 2 available racks
    injector.bindVolatileParameter(MaxNumberOfEvaluators.class, 2); // 1 evaluator per rack
    containerManager = injector.getInstance(ContainerManager.class); // inject containerManager with this updated info
    sendNodeDescriptors();
    resourceManager = new ResourceManager(containerManager, mockResourceAllocationHandler, mockRuntimeStatusHandler,
        DEFAULT_MEMORY_SIZE, DEFAULT_CORES, availableRacksSet, JVM_HEAP_SLACK, configurationSerializer, remoteManager,
        filenames, loggingScopeFactory);
    final ResourceRequestEvent request = ResourceRequestEventImpl.newBuilder().setResourceCount(2).setVirtualCores(1)
        .setMemorySize(64).setRelaxLocality(Boolean.TRUE).addRackName(availableRacks.get(0)).addRackName(availableRacks.get(1)).build();
    // When
    resourceManager.onResourceRequest(request);
    // Then
    verify(mockResourceAllocationHandler, times(2)).onNext(any(ResourceAllocationEvent.class));
    verify(mockRuntimeStatusHandler, times(3)).onNext(any(RuntimeStatusEvent.class));
  }

  @Test
  public void testTwoAllocationsOnFourContainersAvailableInDefaultRack() throws InjectionException {
    // Given
    containerManager = injector.getInstance(ContainerManager.class);
    sendNodeDescriptors();
    final Set<String> availableRacks = new HashSet<String>(Arrays.asList(RackNames.DEFAULT_RACK_NAME));
    resourceManager = new ResourceManager(containerManager, mockResourceAllocationHandler, mockRuntimeStatusHandler,
        DEFAULT_MEMORY_SIZE, DEFAULT_CORES, availableRacks, JVM_HEAP_SLACK, configurationSerializer, remoteManager,
        filenames, loggingScopeFactory);
    final ResourceRequestEvent request = ResourceRequestEventImpl.newBuilder().setResourceCount(2).setVirtualCores(1)
        .setMemorySize(64).build();
    // When
    resourceManager.onResourceRequest(request);
    // Then
    verify(mockResourceAllocationHandler, times(2)).onNext(any(ResourceAllocationEvent.class));
    verify(mockRuntimeStatusHandler, times(3)).onNext(any(RuntimeStatusEvent.class));
  }

}
