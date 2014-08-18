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
package com.microsoft.reef.runtime.local;

import com.microsoft.reef.runtime.common.files.RuntimeClasspathProvider;

import javax.inject.Inject;
import java.util.Collections;
import java.util.List;

/**
 * RuntimeClasspathProvider for the local runtime.
 */
public final class LocalClasspathProvider implements RuntimeClasspathProvider {

  @Inject
  LocalClasspathProvider() {
  }

  @Override
  public List<String> getDriverClasspath() {
    return Collections.emptyList();
  }

  @Override
  public List<String> getEvaluatorClasspath() {
    return Collections.emptyList();
  }
}
