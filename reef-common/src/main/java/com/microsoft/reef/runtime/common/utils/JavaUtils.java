/**
 * Copyright (C) 2013 Microsoft Corporation
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
package com.microsoft.reef.runtime.common.utils;

import com.microsoft.reef.util.OSUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author mweimer
 */
public class JavaUtils {

  public static String getClasspath() {
    try {
      return System.getProperty("java.class.path");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @return the full path to the java(.exe) binary
   */
  public static String getJavaBinary() {

    final String java;
    if (OSUtils.isWindows()) {
      java = "java.exe";
    } else {
      java = "java";
    }

    // Set java executable command
    final String javaHome = System.getenv("JAVA_HOME");

    if (javaHome != null) {
      return javaHome + "/bin/" + java;
    } else {
      return java;
    }
  }

  public static List<String> getEnvironmentList() {
    final Map<String, String> env = System.getenv();

    final ArrayList<String> envList = new ArrayList<>();
    for (Map.Entry<String, String> entry : env.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      envList.add(key + "='" + value + "'");
    }
    return envList;
  }
}
