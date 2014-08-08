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
package com.microsoft.reef.io.network.group.operators;

import com.microsoft.tang.annotations.Name;

public abstract class AbstractGroupCommOperator implements GroupCommOperator {

  @Override
  public Class<? extends Name<String>> getOperName() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Class<? extends Name<String>> getGroupName() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void initialize() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getVersion() {
    throw new UnsupportedOperationException();
  }
}
