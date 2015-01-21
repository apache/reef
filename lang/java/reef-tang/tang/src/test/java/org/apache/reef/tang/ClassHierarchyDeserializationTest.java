/**
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
 */
public class ClassHierarchyDeserializationTest {

  @Test
  public void testDeserializationForTasks() {
    try (final InputStream chin = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream("Task.bin")) {
      final ClassHierarchyProto.Node root = ClassHierarchyProto.Node.parseFrom(chin); // A
      final ClassHierarchy ch = new ProtocolBufferClassHierarchy(root);
      Node n1 = ch.getNode("Microsoft.Reef.Tasks.StreamTask1, Microsoft.Reef.Tasks, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null");
      Assert.assertTrue(n1.getFullName().equals("Microsoft.Reef.Tasks.StreamTask1, Microsoft.Reef.Tasks, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null"));

      Node n2 = ch.getNode("Microsoft.Reef.Tasks.HelloTask, Microsoft.Reef.Tasks, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null");
      Assert.assertTrue(n2.getFullName().equals("Microsoft.Reef.Tasks.HelloTask, Microsoft.Reef.Tasks, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null"));

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

  @Test
  public void testDeserializationForEvent() {
    try (final InputStream chin = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream("Event.bin")) {
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
  //Test bindSetEntry(NamedParameterNode<Set<T>> iface, String impl) in ConfigurationBuilderImpl with deserialized class hierarchy
  public void testBindSetEntryWithSetOfT() throws IOException {
    final ClassHierarchy ns1 = Tang.Factory.getTang().getDefaultClassHierarchy();
    ns1.getNode(SetOfClasses.class.getName());
    final ClassHierarchy ns2 = new ProtocolBufferClassHierarchy(ProtocolBufferClassHierarchy.serialize(ns1));
    final ConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder(ns2);

    final NamedParameterNode<Set<Number>> n2 = (NamedParameterNode<Set<Number>>) ns1.getNode(SetOfClasses.class.getName());
    final Node fn = ns1.getNode(Float.class.getName());
    cb.bindSetEntry(n2, fn);

    final ConfigurationSerializer serializer = new AvroConfigurationSerializer();
    final Configuration c = serializer.fromString(serializer.toString(cb.build()), ns2);
  }

  @Test
  //Test public <T> void bindParameter(NamedParameterNode<T> name, String value) in ConfigurationBuilderImpl with deserialized class hierarchy
  public void testBindSetEntryWithSetOfString() throws IOException {
    final ClassHierarchy ns1 = Tang.Factory.getTang().getDefaultClassHierarchy();
    ns1.getNode(SetOfStrings.class.getName());
    final ClassHierarchy ns2 = new ProtocolBufferClassHierarchy(ProtocolBufferClassHierarchy.serialize(ns1));
    final ConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder(ns2);
    cb.bindSetEntry(SetOfStrings.class.getName(), "four");
    cb.bindSetEntry(SetOfStrings.class.getName(), "five");

    final NamedParameterNode<Set<String>> n2 = (NamedParameterNode<Set<String>>) ns1.getNode(SetOfStrings.class.getName());
    cb.bindSetEntry(n2, "six");

    final ConfigurationSerializer serializer = new AvroConfigurationSerializer();
    final Configuration c = serializer.fromString(serializer.toString(cb.build()), ns2);
  }
}
