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

import org.apache.reef.runtime.common.driver.api.*;
import org.apache.reef.runtime.common.driver.resourcemanager.NodeDescriptorEvent;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceAllocationEvent;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceStatusEvent;
import org.apache.reef.runtime.common.driver.resourcemanager.RuntimeStatusEvent;
import org.apache.reef.runtime.local.driver.*;
import org.apache.reef.runtime.multi.driver.RuntimesHost;
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

import java.util.HashSet;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Tests for RuntimesHost.
 */
public class RuntimesHostTest {
  private Injector injector;
  private Queue<Object> commandsQueue = new LinkedBlockingQueue<>();

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
    this.commandsQueue.clear();
  }

  @Test(expected = RuntimeException.class)
  public void testRuntimesHostInitializedWithEmptyRuntimesList() {
    HashSet<String> runtimes = new HashSet<>();
    new RuntimesHost(injector, runtimes);
  }

  @Test(expected = RuntimeException.class)
  public void testRuntimesHostInitializedWithCorruptedRuntimesList() {
    HashSet<String> runtimes = new HashSet<>();
    runtimes.add("corrupted avro");
    final RuntimesHost rHost = new RuntimesHost(injector, runtimes);
    rHost.onNext(new RuntimeStart(System.currentTimeMillis()));
  }

  @Test(expected = RuntimeException.class)
  public void testRuntimesHostInitializedWithMissingNodeDescriptorHandler() {
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
    HashSet<String> runtimes = new HashSet<>();
    runtimes.add(config);
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

    final RuntimesHost rHost = new RuntimesHost(badInjector, runtimes);
    rHost.onNext(new RuntimeStart(System.currentTimeMillis()));
  }

  @Test(expected = RuntimeException.class)
  public void testRuntimesHostInitializedWithMissingResourceStatusHandler() {
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
    HashSet<String> runtimes = new HashSet<>();
    runtimes.add(config);
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

    final RuntimesHost rHost = new RuntimesHost(badInjector, runtimes);
    rHost.onNext(new RuntimeStart(System.currentTimeMillis()));
  }

  @Test(expected = RuntimeException.class)
  public void testRuntimesHostInitializedWithMissingRuntimeStatusHandler() {
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
    HashSet<String> runtimes = new HashSet<>();
    runtimes.add(config);
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

    final RuntimesHost rHost = new RuntimesHost(badInjector, runtimes);
    rHost.onNext(new RuntimeStart(System.currentTimeMillis()));
  }

  @Test(expected = RuntimeException.class)
  public void testRuntimesHostInitializedWithMissingResourceAllocationHandler() {
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
    HashSet<String> runtimes = new HashSet<>();
    runtimes.add(config);
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

    final RuntimesHost rHost = new RuntimesHost(badInjector, runtimes);
    rHost.onNext(new RuntimeStart(System.currentTimeMillis()));
  }

  @Test
  public void testRuntimesHostRoutedToDefaultRuntime() {
    Configuration driverConfiguration = TestDriverConfiguration.CONF.build();
    AvroConfigurationSerializer serializer = new AvroConfigurationSerializer();
    String config = serializer.toString(driverConfiguration);
    HashSet<String> runtimes = new HashSet<>();
    runtimes.add(config);
    final RuntimesHost rHost = new RuntimesHost(injector, runtimes);
    rHost.onNext(new RuntimeStart(System.currentTimeMillis()));
    Assert.assertEquals(1, this.commandsQueue.size());
    Object obj = commandsQueue.poll();
    Assert.assertTrue(obj instanceof RuntimeStatusEvent);
  }

  class TestResourceStatusHandler implements EventHandler<ResourceStatusEvent> {
    @Override
    public void onNext(final ResourceStatusEvent value) {
      RuntimesHostTest.this.commandsQueue.add(value);
    }
  }

  class TestRuntimeStatusHandler implements EventHandler<RuntimeStatusEvent> {
    @Override
    public void onNext(final RuntimeStatusEvent value) {
      RuntimesHostTest.this.commandsQueue.add(value);
    }
  }

  class TestNodeDescriptorHandler implements EventHandler<NodeDescriptorEvent> {
    @Override
    public void onNext(final NodeDescriptorEvent value) {
      RuntimesHostTest.this.commandsQueue.add(value);
    }
  }

  class TestResourceAllocationHandler implements EventHandler<ResourceAllocationEvent> {
    @Override
    public void onNext(final ResourceAllocationEvent value) {
      RuntimesHostTest.this.commandsQueue.add(value);
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
    @Override
    public void onNext(final ResourceLaunchEvent value) {

    }
  }

  private static class TestResourceRequestHandler implements ResourceRequestHandler {
    @Override
    public void onNext(final ResourceRequestEvent value) {

    }
  }

  private static class TestResourceReleaseHandler implements ResourceReleaseHandler {
    @Override
    public void onNext(final ResourceReleaseEvent value) {

    }
  }

  private static class TestResourceManagerStartHandler implements ResourceManagerStartHandler {
    @Override
    public void onNext(final RuntimeStart value) {

    }
  }

  private static class TestResourceManagerStopHandler implements ResourceManagerStopHandler {
    @Override
    public void onNext(final RuntimeStop value) {

    }
  }
}
