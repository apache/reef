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
package org.apache.reef.util;

/**
 * Utilities for creating Builders.
 */
public final class BuilderUtils {
  /**
   * Throws a runtime exception if the parameter is null.
   * @param <T> a type of parameter
   * @param parameter
   * @return the parameter if it is not null
   */
  public static <T> T notNull(final T parameter) {
    if (parameter == null) {
      throw new IllegalArgumentException("required parameter");
    } else {
      return parameter;
    }
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private BuilderUtils() {
  }
}
