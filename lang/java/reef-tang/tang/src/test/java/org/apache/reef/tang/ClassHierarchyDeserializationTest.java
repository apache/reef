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
package org.apache.reef.tang;

import org.apache.reef.tang.exceptions.NameResolutionException;
import org.apache.reef.tang.implementation.avro.AvroClassHierarchySerializer;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.tang.implementation.protobuf.ProtocolBufferClassHierarchy;
import org.apache.reef.tang.proto.ClassHierarchyProto;
import org.apache.reef.tang.types.NamedParameterNode;
import org.apache.reef.tang.types.Node;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

/**
 * Test case for class hierarchy deserialization.
 * TODO: The files should be created and deserialized by the AvroClassHierarchySerializer (REEF-400)
 */
public class ClassHierarchyDeserializationTest {
  private final ConfigurationSerializer configurationSerializer = new AvroConfigurationSerializer();
  private final ClassHierarchySerializer classHierarchySerializer = new AvroClassHierarchySerializer();

  /**
   * generate task.bin from running .Net ClassHierarchyBuilder.exe.
   */
  @Test
  public void testDeserializationForTasks() {
    // TODO: The file should be written by Avro (REEF-400)
    try (InputStream chin = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream("Task.bin")) {
      // TODO: Use AvroClassHierarchySerializer instead (REEF-400)
      final ClassHierarchyProto.Node root = ClassHierarchyProto.Node.parseFrom(chin);
      final ClassHierarchy ch = new ProtocolBufferClassHierarchy(root);
      final Node n1 = ch.getNode("Org.Apache.REEF.Examples.Tasks.StreamingTasks.StreamTask1, " +
          "Org.Apache.REEF.Examples.Tasks, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null");
      Assert.assertTrue(n1.getFullName().equals("Org.Apache.REEF.Examples.Tasks.StreamingTasks.StreamTask1, " +
          "Org.Apache.REEF.Examples.Tasks, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null"));

      final Node n2 = ch.getNode("Org.Apache.REEF.Examples.Tasks.HelloTask.HelloTask, " +
          "Org.Apache.REEF.Examples.Tasks, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null");
      Assert.assertTrue(n2.getFullName().equals("Org.Apache.REEF.Examples.Tasks.HelloTask.HelloTask, " +
          "Org.Apache.REEF.Examples.Tasks, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null"));

      final ConfigurationBuilder taskConfigurationBuilder1 = Tang.Factory.getTang()
          .newConfigurationBuilder(ch);

      final ConfigurationBuilder taskConfigurationBuilder2 = Tang.Factory.getTang()
          .newConfigurationBuilder(ch);
    } catch (final IOException e) {
      final String message = "Unable to load class hierarchy.";
      throw new RuntimeException(message, e);
    } catch (final NameResolutionException e) {
      final String message = "Unable to get node from class hierarchy.";
      throw new RuntimeException(message, e);
    }
  }

  /**
   * This is to test CLR protocol Buffer class hierarchy merge.
   */
  @Test
  public void testProtocolClassHierarchyMerge() {
    final ConfigurationBuilder taskConfigurationBuilder;
    final ConfigurationBuilder eventConfigurationBuilder;

    // TODO: The file should be written by Avro (REEF-400)
    try (InputStream chin = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream("Task.bin")) {
      // TODO: Use AvroClassHierarchySerializer instead (REEF-400)
      final ClassHierarchyProto.Node root = ClassHierarchyProto.Node.parseFrom(chin);
      final ClassHierarchy ch = new ProtocolBufferClassHierarchy(root);
      taskConfigurationBuilder = Tang.Factory.getTang().newConfigurationBuilder(ch);
    } catch (final IOException e) {
      final String message = "Unable to load class hierarchy from task.bin.";
      throw new RuntimeException(message, e);
    }

    // TODO: The file should be written by Avro (REEF-400)
    try (InputStream chin = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream("Event.bin")) {
      // TODO: Use AvroClassHierarchySerializer instead (REEF-400)
      final ClassHierarchyProto.Node root = ClassHierarchyProto.Node.parseFrom(chin);
      final ClassHierarchy ch = new ProtocolBufferClassHierarchy(root);
      eventConfigurationBuilder = Tang.Factory.getTang().newConfigurationBuilder(ch);
    } catch (final Exception e) {
      final String message = "Unable to load class hierarchy from event.bin.";
      throw new RuntimeException(message, e);
    }

    taskConfigurationBuilder.addConfiguration(eventConfigurationBuilder.build());
  }

  /**
   * generate event.bin from .Net Tang test case TestSerilization.TestGenericClass.
   */
  @Test
  public void testDeserializationForEvent() {
    // TODO: The file should be written by Avro (REEF-400)
    try (InputStream chin = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream("Event.bin")) {
      // TODO: Use AvroClassHierarchySerializer instead (REEF-400)
      final ClassHierarchyProto.Node root = ClassHierarchyProto.Node.parseFrom(chin);
      final ClassHierarchy ch = new ProtocolBufferClassHierarchy(root);
      final ConfigurationBuilder taskConfigurationBuilder = Tang.Factory.getTang()
          .newConfigurationBuilder(ch);
    } catch (final Exception e) {
      final String message = "Unable to load class hierarchy.";
      throw new RuntimeException(message, e);
    }
  }

  @Test
  // Test bindSetEntry(NamedParameterNode<Set<T>> iface, String impl) in ConfigurationBuilderImpl
  // with deserialized class hierarchy
  public void testBindSetEntryWithSetOfT() throws IOException {
    final ClassHierarchy ns1 = Tang.Factory.getTang().getDefaultClassHierarchy();
    ns1.getNode(SetOfClasses.class.getName());
    final ClassHierarchy ns2 = classHierarchySerializer.fromString(classHierarchySerializer.toString(ns1));
    final ConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder(ns2);

    final NamedParameterNode<Set<Number>> n2 =
        (NamedParameterNode<Set<Number>>) ns1.getNode(SetOfClasses.class.getName());
    final Node fn = ns1.getNode(Float.class.getName());
    cb.bindSetEntry(n2, fn);

    final Configuration c = configurationSerializer.fromString(configurationSerializer.toString(cb.build()), ns2);
  }

  @Test
  // Test public <T> void bindParameter(NamedParameterNode<T> name, String value) in ConfigurationBuilderImpl
  // with deserialized class hierarchy
  public void testBindSetEntryWithSetOfString() throws IOException {
    final ClassHierarchy ns1 = Tang.Factory.getTang().getDefaultClassHierarchy();
    ns1.getNode(SetOfStrings.class.getName());
    final ClassHierarchy ns2 = classHierarchySerializer.fromString(classHierarchySerializer.toString(ns1));
    final ConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder(ns2);
    cb.bindSetEntry(SetOfStrings.class.getName(), "four");
    cb.bindSetEntry(SetOfStrings.class.getName(), "five");

    final NamedParameterNode<Set<String>> n2 =
        (NamedParameterNode<Set<String>>) ns1.getNode(SetOfStrings.class.getName());
    cb.bindSetEntry(n2, "six");

    final Configuration c = configurationSerializer.fromString(configurationSerializer.toString(cb.build()), ns2);
  }
}
