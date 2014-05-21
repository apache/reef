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
package com.microsoft.tang;

import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.NameResolutionException;
import com.microsoft.tang.implementation.protobuf.ProtocolBufferClassHierarchy;
import com.microsoft.tang.proto.ClassHierarchyProto;
import com.microsoft.tang.types.Node;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;

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
          Node n1 = ch.getNode("Microsoft.Reef.Tasks.StreamTask1, Microsoft.Reef.Tasks.StreamingTask, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null");
          Assert.assertTrue(n1.getFullName().equals("Microsoft.Reef.Tasks.StreamTask1, Microsoft.Reef.Tasks.StreamingTask, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null"));

          Node n2 = ch.getNode("Microsoft.Reef.Tasks.HelloTask, Microsoft.Reef.Tasks.HelloTask, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null");
          Assert.assertTrue(n2.getFullName().equals("Microsoft.Reef.Tasks.HelloTask, Microsoft.Reef.Tasks.HelloTask, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null"));

          final ConfigurationBuilder taskConfigurationBuilder1 = Tang.Factory.getTang()
                  .newConfigurationBuilder(ch);

          final ConfigurationBuilder taskConfigurationBuilder2 = Tang.Factory.getTang()
                  .newConfigurationBuilder(ch);
          try {
            taskConfigurationBuilder1.bind("Microsoft.Reef.Tasks.TaskConfigurationOptions+Identifier, Microsoft.Reef.Tasks.ITask, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null", "Hello_From_Streaming1");
            taskConfigurationBuilder1.bind("Microsoft.Reef.Tasks.ITask, Microsoft.Reef.Tasks.ITask, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null", "Microsoft.Reef.Tasks.StreamTask1, Microsoft.Reef.Tasks.StreamingTask, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null");
            taskConfigurationBuilder1.build();

            taskConfigurationBuilder2.bind("Microsoft.Reef.Tasks.TaskConfigurationOptions+Identifier, Microsoft.Reef.Tasks.ITask, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null", "Hello_From_HelloTask");
            taskConfigurationBuilder2.bind("Microsoft.Reef.Tasks.ITask, Microsoft.Reef.Tasks.ITask, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null", "Microsoft.Reef.Tasks.HelloTask, Microsoft.Reef.Tasks.HelloTask, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null");
            taskConfigurationBuilder2.build();
          } catch (final BindException ex) {
            final String message = "Unable to setup Task or Context configuration.";
            throw new RuntimeException(message, ex);
          }
      } catch (final IOException e) {
          final String message = "Unable to load class hierarchy.";
          throw new RuntimeException(message, e);
      }  catch (final NameResolutionException e) {
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
        } catch (final IOException e) {
            final String message = "Unable to load class hierarchy.";
            throw new RuntimeException(message, e);
        }
    }
}
