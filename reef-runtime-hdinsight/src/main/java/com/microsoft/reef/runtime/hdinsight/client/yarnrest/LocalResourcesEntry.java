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
package com.microsoft.reef.runtime.hdinsight.client.yarnrest;

public final class LocalResourcesEntry {

  private String key;
  private FileResource value;

  public LocalResourcesEntry(final String key, final FileResource value) {
    this.key = key;
    this.value = value;
  }

  public String getKey() {
    return this.key;
  }

  public LocalResourcesEntry setKey(final String key) {
    this.key = key;
    return this;
  }

  public FileResource getValue() {
    return this.value;
  }

  public LocalResourcesEntry setValue(final FileResource value) {
    this.value = value;
    return this;
  }
}
