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

/**
 * This interface is used to track the source of configuration bindings.
 * <p/>
 * This can be explicitly set (such as by configuration file parsers), or be
 * implicitly bound to the stack trace of bind() invocation.
 */
public interface BindLocation {
  /**
   * Implementations of BindLocation should override toString() so that it
   * returns a human readable representation of the source of the
   * configuration option in question.
   *
   * @return A (potentially multi-line) string that represents the stack
   * trace, configuration file location, or other source of some
   * configuration data.
   */
  public abstract String toString();
}
