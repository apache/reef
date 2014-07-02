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
package com.microsoft.reef.examples.hello;

import com.microsoft.reef.runtime.hdinsight.client.UnsafeHDInsightRuntimeConfiguration;
import com.microsoft.tang.exceptions.InjectionException;

import java.io.IOException;

/**
 * HelloREEF running on HDInsight
 */
public class HelloHDInsight {
  public static void main(final String[] args) throws InjectionException, IOException {
    HelloREEFNoClient.runHelloReefWithoutClient(UnsafeHDInsightRuntimeConfiguration.fromEnvironment());
  }
}
