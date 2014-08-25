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
package com.microsoft.reef.tests.library.tasks;

import com.microsoft.reef.task.Task;

import javax.inject.Inject;

/**
 * A Task that just sends the memento back.
 */
public final class EchoTask implements Task {

  @Inject
  private EchoTask() {
  }

  @Override
  public byte[] call(byte[] memento) throws Exception {
    return memento;
  }
}
