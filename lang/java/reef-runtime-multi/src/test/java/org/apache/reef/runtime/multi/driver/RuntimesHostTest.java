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

//*
package org.apache.reef.runtime.multi.driver;

import org.apache.reef.runtime.common.driver.api.*;
import org.apache.reef.runtime.common.driver.resourcemanager.NodeDescriptorEvent;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceAllocationEvent;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceStatusEvent;
import org.apache.reef.runtime.common.driver.resourcemanager.RuntimeStatusEvent;
import org.apache.reef.runtime.local.driver.*;
import org.apache.reef.runtime.multi.client.parameters.SerializedRuntimeDefinitions;
import org.apache.reef.runtime.multi.utils.RuntimeDefinitionSerializer;
import org.apache.reef.runtime.multi.utils.avro.RuntimeDefinition;
import org.apache.reef.tang.*;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.runtime.event.RuntimeStart;
import org.apache.reef.wake.time.runtime.event.RuntimeStop;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.inject.Inject;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Tests for RuntimesHost.
 */
public class RuntimesHostTest {
  private Injector injector;
  private static Queue<Object> commandsQueue = new LinkedBlockingQueue<>();

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() throws InjectionException {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindNamedParameter(
            RuntimeParameters.NodeDescriptorHandler.class,
            RuntimesHostTest.TestNodeDescriptorHandler.class);
    cb.bindNamedParameter(
            RuntimeParameters.ResourceStatusHandler.class,
            RuntimesHostTest.TestResourceStatusHandler.class);
    cb.bindNamedParameter(
            RuntimeParameters.RuntimeStatusHandler.class,
            RuntimesHostTest.TestRuntimeStatusHandler.class);
    cb.bindNamedParameter(
            RuntimeParameters.ResourceAllocationHandler.class,
            RuntimesHostTest.TestResourceAllocationHandler.class);
    this.injector = Tang.Factory.getTang().newInjector(cb.build());
    RuntimesHostTest.commandsQueue.clear();
  }

  @Test(expected = RuntimeException.class)
  public void testRuntimesHostInitializedWithCorruptedRuntimesList() throws InjectionException {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindSetEntry(
            SerializedRuntimeDefinitions.class,
            "corrupted avro");

    Injector forked = injector.forkInjector(cb.build());
    final RuntimesHost rHost = forked.getInstance(RuntimesHost.class);
    rHost.onRuntimeStart(new RuntimeStart(System.currentTimeMillis()));
  }

  @Test(expected = RuntimeException.class)
  public void testRuntimesHostInitializedWithMissingNodeDescriptorHandler() throws InjectionException {
    ConfigurationModule configModule = LocalDriverConfiguration.CONF
            .set(LocalDriverConfiguration.MAX_NUMBER_OF_EVALUATORS, 1)
            .set(LocalDriverConfiguration.ROOT_FOLDER, "")
            .set(LocalDriverConfiguration.JVM_HEAP_SLACK, 1024)
            .set(LocalDriverConfiguration.CLIENT_REMOTE_IDENTIFIER, "ID")
            .set(LocalDriverConfiguration.JOB_IDENTIFIER, "ID");
    configModule = configModule.set(LocalDriverConfiguration.RACK_NAMES, "");

    Configuration localDriverConfiguration = configModule.build();
    AvroConfigurationSerializer serializer = new AvroConfigurationSerializer();
    String config = serializer.toString(localDriverConfiguration);
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindNamedParameter(
            RuntimeParameters.ResourceStatusHandler.class,
            RuntimesHostTest.TestResourceStatusHandler.class);
    cb.bindNamedParameter(
            RuntimeParameters.RuntimeStatusHandler.class,
            RuntimesHostTest.TestRuntimeStatusHandler.class);
    cb.bindNamedParameter(
            RuntimeParameters.ResourceAllocationHandler.class,
            RuntimesHostTest
            .TestResourceAllocationHandler.class);
    Injector badInjector = Tang.Factory.getTang().newInjector(cb.build());

    final JavaConfigurationBuilder cbtest = Tang.Factory.getTang().newConfigurationBuilder();
    cbtest.bindSetEntry(
            SerializedRuntimeDefinitions.class,
            config);

    Injector forked = badInjector.forkInjector(cbtest.build());
    final RuntimesHost rHost = forked.getInstance(RuntimesHost.class);
    rHost.onRuntimeStart(new RuntimeStart(System.currentTimeMillis()));
  }

  @Test(expected = RuntimeException.class)
  public void testRuntimesHostInitializedWithMissingResourceStatusHandler() throws InjectionException {
    ConfigurationModule configModule = LocalDriverConfiguration.CONF
            .set(LocalDriverConfiguration.MAX_NUMBER_OF_EVALUATORS, 1)
            .set(LocalDriverConfiguration.ROOT_FOLDER, "")
            .set(LocalDriverConfiguration.JVM_HEAP_SLACK, 1024)
            .set(LocalDriverConfiguration.CLIENT_REMOTE_IDENTIFIER, "ID")
            .set(LocalDriverConfiguration.JOB_IDENTIFIER, "ID");
    configModule = configModule.set(LocalDriverConfiguration.RACK_NAMES, "");

    Configuration localDriverConfiguration = configModule.build();
    AvroConfigurationSerializer serializer = new AvroConfigurationSerializer();
    String config = serializer.toString(localDriverConfiguration);
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindNamedParameter(
            RuntimeParameters.NodeDescriptorHandler.class,
            RuntimesHostTest.TestNodeDescriptorHandler.class);
    cb.bindNamedParameter(
            RuntimeParameters.RuntimeStatusHandler.class,
            RuntimesHostTest.TestRuntimeStatusHandler.class);
    cb.bindNamedParameter(
            RuntimeParameters.ResourceAllocationHandler.class,
            RuntimesHostTest.TestResourceAllocationHandler.class);
    Injector badInjector = Tang.Factory.getTang().newInjector(cb.build());

    final JavaConfigurationBuilder cbtest = Tang.Factory.getTang().newConfigurationBuilder();
    cbtest.bindSetEntry(
            SerializedRuntimeDefinitions.class,
            config);

    Injector forked = badInjector.forkInjector(cbtest.build());
    final RuntimesHost rHost = forked.getInstance(RuntimesHost.class);
    rHost.onRuntimeStart(new RuntimeStart(System.currentTimeMillis()));
  }

  @Test(expected = RuntimeException.class)
  public void testRuntimesHostInitializedWithMissingRuntimeStatusHandler() throws InjectionException {
    ConfigurationModule configModule = LocalDriverConfiguration.CONF
            .set(LocalDriverConfiguration.MAX_NUMBER_OF_EVALUATORS, 1)
            .set(LocalDriverConfiguration.ROOT_FOLDER, "")
            .set(LocalDriverConfiguration.JVM_HEAP_SLACK, 1024)
            .set(LocalDriverConfiguration.CLIENT_REMOTE_IDENTIFIER, "ID")
            .set(LocalDriverConfiguration.JOB_IDENTIFIER, "ID");
    configModule = configModule.set(LocalDriverConfiguration.RACK_NAMES, "");

    Configuration localDriverConfiguration = configModule.build();
    AvroConfigurationSerializer serializer = new AvroConfigurationSerializer();
    String config = serializer.toString(localDriverConfiguration);
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindNamedParameter(
            RuntimeParameters.NodeDescriptorHandler.class,
            RuntimesHostTest.TestNodeDescriptorHandler.class);
    cb.bindNamedParameter(
            RuntimeParameters.ResourceStatusHandler.class,
            RuntimesHostTest.TestResourceStatusHandler.class);
    cb.bindNamedParameter(
            RuntimeParameters.ResourceAllocationHandler.class,
            RuntimesHostTest.TestResourceAllocationHandler.class);
    Injector badInjector = Tang.Factory.getTang().newInjector(cb.build());

    final JavaConfigurationBuilder cbtest = Tang.Factory.getTang().newConfigurationBuilder();
    cbtest.bindSetEntry(
            SerializedRuntimeDefinitions.class,
            config);

    Injector forked = badInjector.forkInjector(cbtest.build());
    final RuntimesHost rHost = forked.getInstance(RuntimesHost.class);
    rHost.onRuntimeStart(new RuntimeStart(System.currentTimeMillis()));
  }

  @Test(expected = RuntimeException.class)
  public void testRuntimesHostInitializedWithMissingResourceAllocationHandler() throws InjectionException {
    ConfigurationModule configModule = LocalDriverConfiguration.CONF
            .set(LocalDriverConfiguration.MAX_NUMBER_OF_EVALUATORS, 1)
            .set(LocalDriverConfiguration.ROOT_FOLDER, "")
            .set(LocalDriverConfiguration.JVM_HEAP_SLACK, 1024)
            .set(LocalDriverConfiguration.CLIENT_REMOTE_IDENTIFIER, "ID")
            .set(LocalDriverConfiguration.JOB_IDENTIFIER, "ID");
    configModule = configModule.set(LocalDriverConfiguration.RACK_NAMES, "");

    Configuration localDriverConfiguration = configModule.build();
    AvroConfigurationSerializer serializer = new AvroConfigurationSerializer();
    String config = serializer.toString(localDriverConfiguration);
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindNamedParameter(
            RuntimeParameters.NodeDescriptorHandler.class,
            RuntimesHostTest.TestNodeDescriptorHandler.class);
    cb.bindNamedParameter(
            RuntimeParameters.ResourceStatusHandler.class,
            RuntimesHostTest.TestResourceStatusHandler.class);
    cb.bindNamedParameter(
            RuntimeParameters.RuntimeStatusHandler.class,
            RuntimesHostTest.TestRuntimeStatusHandler.class);
    Injector badInjector = Tang.Factory.getTang().newInjector(cb.build());

    final JavaConfigurationBuilder cbtest = Tang.Factory.getTang().newConfigurationBuilder();
    cbtest.bindSetEntry(
            SerializedRuntimeDefinitions.class,
            config);

    Injector forked = badInjector.forkInjector(cbtest.build());
    final RuntimesHost rHost = forked.getInstance(RuntimesHost.class);
    rHost.onRuntimeStart(new RuntimeStart(System.currentTimeMillis()));
  }

  @Test
  public void testRuntimesHostRoutedToDefaultRuntime() throws InjectionException {
    Configuration driverConfiguration = TestDriverConfiguration.CONF.build();
    AvroConfigurationSerializer serializer = new AvroConfigurationSerializer();
    String config = serializer.toString(driverConfiguration);
    RuntimeDefinition rd = new RuntimeDefinition();
    rd.setDefaultConfiguration(true);
    rd.setSerializedConfiguration(config);
    rd.setRuntimeName("test");
    final String serializedConfiguration = getRuntimeDefinition(rd);
    final JavaConfigurationBuilder cbtest = Tang.Factory.getTang().newConfigurationBuilder();
    cbtest.bindSetEntry(
            SerializedRuntimeDefinitions.class,
            serializedConfiguration);

    Injector forked = injector.forkInjector(cbtest.build());
    final RuntimesHost rHost = forked.getInstance(RuntimesHost.class);
    rHost.onRuntimeStart(new RuntimeStart(System.currentTimeMillis()));
    Assert.assertEquals(1, RuntimesHostTest.commandsQueue.size());
    Object obj = RuntimesHostTest.commandsQueue.poll();
    Assert.assertTrue(obj instanceof RuntimeStart);
  }

  private String getRuntimeDefinition(final RuntimeDefinition rd) {
    return new RuntimeDefinitionSerializer().serialize(rd);
  }

  static class TestResourceStatusHandler implements EventHandler<ResourceStatusEvent> {
    @Inject
    TestResourceStatusHandler(){}

    @Override
    public void onNext(final ResourceStatusEvent value) {
      RuntimesHostTest.commandsQueue.add(value);
    }
  }

  static class TestRuntimeStatusHandler implements EventHandler<RuntimeStatusEvent> {
    @Inject
    TestRuntimeStatusHandler(){}

    @Override
    public void onNext(final RuntimeStatusEvent value) {
      RuntimesHostTest.commandsQueue.add(value);
    }
  }

  static class TestNodeDescriptorHandler implements EventHandler<NodeDescriptorEvent> {
    @Inject
    TestNodeDescriptorHandler(){}

    @Override
    public void onNext(final NodeDescriptorEvent value) {
      RuntimesHostTest.commandsQueue.add(value);
    }
  }

  static class TestResourceAllocationHandler implements EventHandler<ResourceAllocationEvent> {
    @Inject
    TestResourceAllocationHandler(){}

    @Override
    public void onNext(final ResourceAllocationEvent value) {
      RuntimesHostTest.commandsQueue.add(value);
    }
  }

  static class TestDriverConfiguration extends ConfigurationModuleBuilder {
    public static final ConfigurationModule CONF = new TestDriverConfiguration()
            .bindImplementation(ResourceLaunchHandler.class, TestResourceLaunchHandler.class)
            .bindImplementation(ResourceRequestHandler.class, TestResourceRequestHandler.class)
            .bindImplementation(ResourceReleaseHandler.class, TestResourceReleaseHandler.class)
            .bindImplementation(ResourceManagerStartHandler.class, TestResourceManagerStartHandler.class)
            .bindImplementation(ResourceManagerStopHandler.class, TestResourceManagerStopHandler.class).build();
  }

  private static class TestResourceLaunchHandler implements ResourceLaunchHandler {
    @Inject
    TestResourceLaunchHandler(){}

    @Override
    public void onNext(final ResourceLaunchEvent value) {
      RuntimesHostTest.commandsQueue.add(value);
    }
  }

  private static class TestResourceRequestHandler implements ResourceRequestHandler {
    @Inject
    TestResourceRequestHandler(){}

    @Override
    public void onNext(final ResourceRequestEvent value) {
      RuntimesHostTest.commandsQueue.add(value);
    }
  }

  private static class TestResourceReleaseHandler implements ResourceReleaseHandler {
    @Inject
    TestResourceReleaseHandler(){}

    @Override
    public void onNext(final ResourceReleaseEvent value) {
      RuntimesHostTest.commandsQueue.add(value);
    }
  }

  private static class TestResourceManagerStartHandler implements ResourceManagerStartHandler {
    @Inject
    TestResourceManagerStartHandler(){}

    @Override
    public void onNext(final RuntimeStart value) {
      RuntimesHostTest.commandsQueue.add(value);
    }
  }

  private static class TestResourceManagerStopHandler implements ResourceManagerStopHandler {
    @Inject
    TestResourceManagerStopHandler(){}

    @Override
    public void onNext(final RuntimeStop value) {
      RuntimesHostTest.commandsQueue.add(value);
    }
  }
}
