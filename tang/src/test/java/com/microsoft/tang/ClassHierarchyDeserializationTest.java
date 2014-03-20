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
  public void testDeserialization() {
      try (final InputStream chin = Thread.currentThread().getContextClassLoader()
              .getResourceAsStream("Task.bin")) {
          final ClassHierarchyProto.Node root = ClassHierarchyProto.Node.parseFrom(chin); // A
          final ClassHierarchy ch = new ProtocolBufferClassHierarchy(root);
          Node n = ch.getNode("Microsoft.Reef.Tasks.StreamTask1, Microsoft.Reef.Tasks.StreamingTask, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null");
          Assert.assertTrue(n.getFullName().equals("Microsoft.Reef.Tasks.StreamTask1, Microsoft.Reef.Tasks.StreamingTask, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null"));

          final ConfigurationBuilder taskConfigurationBuilder = Tang.Factory.getTang()
                  .newConfigurationBuilder(ch);
          try {
            taskConfigurationBuilder.bind("Microsoft.Reef.Tasks.TaskConfigurationOptions+Identifier, Microsoft.Reef.Tasks.ITask, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null", "Hello_From_Streaming1");
            taskConfigurationBuilder.bind("Microsoft.Reef.Tasks.ITask, Microsoft.Reef.Tasks.ITask, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null", "Microsoft.Reef.Tasks.StreamTask1, Microsoft.Reef.Tasks.StreamingTask, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null");
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
}
