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
package org.apache.reef.runtime.common.utils;

/**
 * Constants used by different REEF modules.
 */
public final class Constants {

  /**
   * Any modifier. Used as a wildcard to specify that evaluators can be
   * allocated in any rack.
   */
  public static final String ANY_RACK = "*";

  /**
   * Rack path separator. Used to separate the fully qualified rack name of an
   * evaluator, e.g. /dc1/room1/rack1
   */
  public static final String RACK_PATH_SEPARATOR = "/";

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private Constants() {
  }
}
