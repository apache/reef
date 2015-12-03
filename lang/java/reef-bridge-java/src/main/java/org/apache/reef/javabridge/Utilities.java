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
package org.apache.reef.javabridge;

import org.apache.reef.annotations.audience.Interop;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.evaluator.EvaluatorDescriptor;
import org.apache.reef.tang.ClassHierarchy;
import org.apache.reef.tang.implementation.protobuf.ProtocolBufferClassHierarchy;
import org.apache.reef.tang.proto.ClassHierarchyProto;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * CLR/Java bridge utilities.
 */
@Private
@Interop
public final class Utilities {
  public static ClassHierarchy loadClassHierarchy(final String classHierarchyFile) {
    // TODO[JIRA REEF-400] The file should be created via AvroClassHierarchySerializer
    Path p = Paths.get(classHierarchyFile);
    if (!Files.exists(p)) {
      p = Paths.get(System.getProperty("user.dir") + "/reef/global/" + classHierarchyFile);
    }
    if (!Files.exists(p)) {
      throw new RuntimeException("cannot find file " + p.toAbsolutePath());
    }
    // TODO[JIRA REEF-400] Use the AvroClassHierarchySerializer in place of protobuf
    try (final InputStream chin = new FileInputStream(p.toAbsolutePath().toString())) {
      final ClassHierarchyProto.Node root = ClassHierarchyProto.Node.parseFrom(chin);
      final ClassHierarchy ch = new ProtocolBufferClassHierarchy(root);
      return ch;
    } catch (final IOException e) {
      final String message = "Unable to load class hierarchy from " + classHierarchyFile;
      throw new RuntimeException(message, e);
    }
  }

  public static String getEvaluatorDescriptorString(final EvaluatorDescriptor evaluatorDescriptor) {
    final InetSocketAddress socketAddress = evaluatorDescriptor.getNodeDescriptor().getInetSocketAddress();
    return "IP=" + socketAddress.getAddress() + ", Port=" + socketAddress.getPort() + ", HostName=" +
        socketAddress.getHostName() + ", Memory=" + evaluatorDescriptor.getMemory() + ", Core=" +
        evaluatorDescriptor.getNumberOfCores() + ", RuntimeName=" + evaluatorDescriptor.getRuntimeName();
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private Utilities() {
  }
}
