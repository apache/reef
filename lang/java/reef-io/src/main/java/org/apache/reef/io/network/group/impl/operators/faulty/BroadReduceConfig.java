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
package org.apache.reef.io.network.group.impl.operators.faulty;

import org.apache.reef.io.network.group.operators.Reduce;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.Codec;

import java.util.Set;

public class BroadReduceConfig {
  public static final String defaultValue = "NULL";

  @NamedParameter(default_class = StringIdentifierFactory.class)
  public static class IdFactory implements Name<IdentifierFactory> {
  }

  public static class BroadcastConfig {
    @NamedParameter()
    public static class DataCodec implements Name<Codec<?>> {
    }

    public static class Sender {

      @NamedParameter(doc = "Task ID of the operator")
      public static class SelfId implements Name<String> {
      }

      @NamedParameter(doc = "Task ID of the parent of the operator", default_value = defaultValue)
      public static class ParentId implements Name<String> {
      }

      @NamedParameter(doc = "List of child Identifiers that the operator sends to", default_value = defaultValue)
      public static class ChildIds implements Name<Set<String>> {
      }
    }

    public static class Receiver {

      @NamedParameter(doc = "Task ID of the operator")
      public static class SelfId implements Name<String> {
      }

      @NamedParameter(doc = "Task ID of the parent of the operator", default_value = defaultValue)
      public static class ParentId implements Name<String> {
      }

      @NamedParameter(doc = "List of child Identifiers that the operator sends to", default_value = defaultValue)
      public static class ChildIds implements Name<Set<String>> {
      }
    }
  }

  public static class ReduceConfig {
    @NamedParameter()
    public static class DataCodec implements Name<Codec<?>> {
    }

    @NamedParameter()
    public static class ReduceFunction implements
        Name<Reduce.ReduceFunction<?>> {
    }

    public static class Sender {

      @NamedParameter(doc = "Task ID of the operator")
      public static class SelfId implements Name<String> {
      }

      @NamedParameter(doc = "Task ID of the parent of the operator", default_value = defaultValue)
      public static class ParentId implements Name<String> {
      }

      @NamedParameter(doc = "List of child Identifiers that the operator sends to", default_value = defaultValue)
      public static class ChildIds implements Name<Set<String>> {
      }

      @NamedParameter(doc = "Whether gradient from failed task needs to be approximated", default_value = "true")
      public static class ApproximateGradient implements Name<Boolean> {

      }
    }

    public static class Receiver {

      @NamedParameter(doc = "Task ID of the operator")
      public static class SelfId implements Name<String> {
      }

      @NamedParameter(doc = "Task ID of the parent of the operator", default_value = defaultValue)
      public static class ParentId implements Name<String> {
      }

      @NamedParameter(doc = "List of child Identifiers that the operator sends to", default_value = defaultValue)
      public static class ChildIds implements Name<Set<String>> {
      }
    }
  }
}
