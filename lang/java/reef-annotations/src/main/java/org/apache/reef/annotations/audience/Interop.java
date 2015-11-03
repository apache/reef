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
package org.apache.reef.annotations.audience;

/**
 * Indicates that a class, function, or instance variable is used
 * at the Interop layer, and should not be modified or removed without
 * testing, knowing its consequences, and making corresponding changes
 * in C# and C++ code.
 */
public @interface Interop {
  /**
   * @return The C++ files related to the Interop class/function/instance variable.
   * Note that the coverage may not be absolute.
   */
  String[] CppFiles() default "";

  /**
   * @return The C# files related to the interop class/function/instance variable.
   * Note that the coverage may not be absolute.
   */
  String[] CsFiles() default "";
}
