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

import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.reef.runtime.common.driver.api.*;
import org.apache.reef.runtime.common.driver.resourcemanager.NodeDescriptorEvent;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceAllocationEvent;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceStatusEvent;
import org.apache.reef.runtime.common.driver.resourcemanager.RuntimeStatusEvent;
import org.apache.reef.runtime.local.driver.*;
import org.apache.reef.runtime.multi.driver.RuntimesHost;
import org.apache.reef.runtime.multi.utils.RuntimeDefinition;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Unit;
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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Tests for RuntimesHost.
 */
public class RuntimesHostTest {
  private Injector injector;
  public static Queue<Object> CommandsQueue = new LinkedBlockingQueue<>();

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
    RuntimesHostTest.CommandsQueue.clear();
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
    RuntimeDefinition rd = new RuntimeDefinition();
    rd.setDefaultConfiguration(true);
    rd.setSerializedConfiguration(config);
    rd.setRuntimeName("test");
    HashSet<String> runtimes = new HashSet<>();
    final String serializedConfiguration = getRuntimeDefinition(rd);
    runtimes.add(serializedConfiguration);
    final RuntimesHost rHost = new RuntimesHost(injector, runtimes);
    rHost.onNext(new RuntimeStart(System.currentTimeMillis()));
    Assert.assertEquals(1, RuntimesHostTest.CommandsQueue.size());
    Object obj = RuntimesHostTest.CommandsQueue.poll();
    Assert.assertTrue(obj instanceof RuntimeStart);
  }

  private String getRuntimeDefinition(RuntimeDefinition rd) {
    final DatumWriter<RuntimeDefinition> configurationWriter =
            new SpecificDatumWriter<>(RuntimeDefinition.class);
    final String serializedConfiguration;
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      final JsonEncoder encoder = EncoderFactory.get().jsonEncoder(RuntimeDefinition.SCHEMA$, out);
      configurationWriter.write(rd, encoder);
      encoder.flush();
      out.flush();
      serializedConfiguration = out.toString("ISO-8859-1");
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    return serializedConfiguration;
  }

  static class TestResourceStatusHandler implements EventHandler<ResourceStatusEvent> {
    @Inject
    private TestResourceStatusHandler(){}

    @Override
    public void onNext(final ResourceStatusEvent value) {
      RuntimesHostTest.CommandsQueue.add(value);
    }
  }

  static class TestRuntimeStatusHandler implements EventHandler<RuntimeStatusEvent> {
    @Inject
    private TestRuntimeStatusHandler(){}

    @Override
    public void onNext(final RuntimeStatusEvent value) {
      RuntimesHostTest.CommandsQueue.add(value);
    }
  }

  static class TestNodeDescriptorHandler implements EventHandler<NodeDescriptorEvent> {
    @Inject
    private TestNodeDescriptorHandler(){}

    @Override
    public void onNext(final NodeDescriptorEvent value) {
      RuntimesHostTest.CommandsQueue.add(value);
    }
  }

  static class TestResourceAllocationHandler implements EventHandler<ResourceAllocationEvent> {
    @Inject
    TestResourceAllocationHandler(){}

    @Override
    public void onNext(final ResourceAllocationEvent value) {
      RuntimesHostTest.CommandsQueue.add(value);
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
      RuntimesHostTest.CommandsQueue.add(value);
    }
  }

  private static class TestResourceRequestHandler implements ResourceRequestHandler {
    @Inject
    TestResourceRequestHandler(){}

    @Override
    public void onNext(final ResourceRequestEvent value) {
      RuntimesHostTest.CommandsQueue.add(value);
    }
  }

  private static class TestResourceReleaseHandler implements ResourceReleaseHandler {
    @Inject
    TestResourceReleaseHandler(){}

    @Override
    public void onNext(final ResourceReleaseEvent value) {
      RuntimesHostTest.CommandsQueue.add(value);
    }
  }

  private static class TestResourceManagerStartHandler implements ResourceManagerStartHandler {
    @Inject
    TestResourceManagerStartHandler(){}

    @Override
    public void onNext(final RuntimeStart value) {
      RuntimesHostTest.CommandsQueue.add(value);
    }
  }

  private static class TestResourceManagerStopHandler implements ResourceManagerStopHandler {
    @Inject
    TestResourceManagerStopHandler(){}

    @Override
    public void onNext(final RuntimeStop value) {
      RuntimesHostTest.CommandsQueue.add(value);
    }
  }
}
