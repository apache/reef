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
package com.microsoft.reef.runtime.common.evaluator.task;

import com.microsoft.reef.driver.task.TaskConfigurationOptions;
import com.microsoft.reef.task.events.TaskStart;
import com.microsoft.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * Injectable implementation of TaskStart
 */
final class TaskStartImpl implements TaskStart {

  private final String id;

  @Inject
  TaskStartImpl(final @Parameter(TaskConfigurationOptions.Identifier.class) String id) {
    this.id = id;
  }

  @Override
  public String getId() {
    return this.id;
  }
}
