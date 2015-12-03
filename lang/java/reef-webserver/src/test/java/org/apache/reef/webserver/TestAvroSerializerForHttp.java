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
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.junit.Assert;
import org.junit.Test;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test Avro Serializer for http schema.
 */
public class TestAvroSerializerForHttp {

  @Test
  public void driverInfoSerializerInjectionTest() {
    try {
      final DriverInfoSerializer serializer =
          Tang.Factory.getTang().newInjector().getInstance(DriverInfoSerializer.class);
      final ArrayList<AvroReefServiceInfo> services = new ArrayList<>();
      final AvroReefServiceInfo exampleService =
          AvroReefServiceInfo.newBuilder().setServiceName("exampleService").setServiceInfo("serviceInformation")
              .build();
      services.add(exampleService);
      final AvroDriverInfo driverInfo = serializer.toAvro("abc", "xxxxxx", services);
      final String driverInfoString = serializer.toString(driverInfo);
      Assert.assertEquals(driverInfoString, "{\"remoteId\":\"abc\",\"startTime\":\"xxxxxx\"," +
          "\"services\":[{\"serviceName\":\"exampleService\",\"serviceInfo\":\"serviceInformation\"}]}");
    } catch (final InjectionException e) {
      Assert.fail("Not able to inject DriverInfoSerializer");
    }
  }

  @Test
  public void evaluatorInfoSerializerInjectionTest() {
    try {
      final EvaluatorInfoSerializer serializer =
          Tang.Factory.getTang().newInjector().getInstance(EvaluatorInfoSerializer.class);

      final List<String> ids = new ArrayList<>();
      ids.add("abc");
      final EvaluatorDescriptor evaluatorDescriptor =
          Tang.Factory.getTang().newInjector(EvaluatorDescriptorConfig.CONF.build())
              .getInstance(EvaluatorDescriptor.class);
      final Map<String, EvaluatorDescriptor> data = new HashMap<>();
      data.put("abc", evaluatorDescriptor);

      final AvroEvaluatorsInfo evaluatorInfo = serializer.toAvro(ids, data);
      final String evaluatorInfoString = serializer.toString(evaluatorInfo);
      Assert.assertEquals(evaluatorInfoString, "{\"evaluatorsInfo\":[{\"evaluatorId\":\"abc\",\"nodeId\":\"\"," +
          "\"nodeName\":\"mock\",\"memory\":64,\"type\":\"CLR\",\"internetAddress\":\"\",\"runtimeName\":\"Local\"}]}");
    } catch (final InjectionException e) {
      Assert.fail("Not able to inject EvaluatorInfoSerializer");
    }
  }

  @Test
  public void evaluatorListSerializerInjectionTest() {
    try {
      final EvaluatorListSerializer serializer =
          Tang.Factory.getTang().newInjector().getInstance(EvaluatorListSerializer.class);

      final List<String> ids = new ArrayList<>();
      ids.add("abc");
      final EvaluatorDescriptor evaluatorDescriptor =
          Tang.Factory.getTang().newInjector(EvaluatorDescriptorConfig.CONF.build())
              .getInstance(EvaluatorDescriptor.class);
      final Map<String, EvaluatorDescriptor> data = new HashMap<>();
      data.put("abc", evaluatorDescriptor);

      final AvroEvaluatorList evaluatorList = serializer.toAvro(data, 1, "xxxxxx");
      final String evaluatorListString = serializer.toString(evaluatorList);
      Assert.assertEquals(evaluatorListString,
          "{\"evaluators\":[{\"id\":\"abc\",\"name\":\"mock\"}],\"total\":1,\"startTime\":\"xxxxxx\"}");
    } catch (final InjectionException e) {
      Assert.fail("Not able to inject EvaluatorListSerializer");
    }
  }

  /**
   * Configuration Module Builder for EvaluatorDescriptor.
   */
  public static final class EvaluatorDescriptorConfig extends ConfigurationModuleBuilder {
    static final ConfigurationModule CONF = new EvaluatorDescriptorConfig()
        .bindImplementation(EvaluatorDescriptor.class, EvaluatorDescriptorMock.class)
        .bindImplementation(NodeDescriptor.class, NodeDescriptorMock.class)
        .bindImplementation(EvaluatorProcessFactory.class, CLRProcessFactory.class)
        .build();
  }

  static class EvaluatorDescriptorMock implements EvaluatorDescriptor {
    private final NodeDescriptor nodeDescriptor;
    private final EvaluatorProcessFactory evaluatorProcessFactory;

    @Inject
    EvaluatorDescriptorMock(final NodeDescriptor nodeDescriptor,
                            final EvaluatorProcessFactory evaluatorProcessFactory) {
      this.nodeDescriptor = nodeDescriptor;
      this.evaluatorProcessFactory = evaluatorProcessFactory;
    }

    /**
     * @return the NodeDescriptor of the node where this Evaluator is running.
     */
    @Override
    public NodeDescriptor getNodeDescriptor() {
      return nodeDescriptor;
    }

    /**
     * @return the Evaluator process.
     */
    @Override
    public EvaluatorProcess getProcess() {
      return evaluatorProcessFactory.newEvaluatorProcess();
    }

    /**
     * @return the amount of memory allocated to this Evaluator.
     */
    @Override
    public int getMemory() {
      return 64;
    }

    public int getNumberOfCores() {
      return 1;
    }

    @Override
    public String getRuntimeName() {
      return "Local";
    }
  }

  static class NodeDescriptorMock implements NodeDescriptor {
    @Inject
    NodeDescriptorMock() {
    }

    /**
     * Access the inet address of the Evaluator.
     *
     * @return the inet address of the Evaluator.
     */
    @Override
    public InetSocketAddress getInetSocketAddress() {
      return null;
    }

    /**
     * @return the rack descriptor that contains this node
     */
    @Override
    public RackDescriptor getRackDescriptor() {
      return null;
    }

    @Override
    public String getName() {
      return "mock";
    }

    /**
     * Returns an identifier of this object.
     *
     * @return an identifier of this object
     */
    @Override
    public String getId() {
      return null;
    }
  }
}
