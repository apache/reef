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

public final class FileResource {
  private String url;
  private String type;
  public static final String TYPE_FILE = "FILE";
  public static final String TYPE_ARCHIVE = "ARCHIVE";
  private String visibility;
  public static final String VISIBILITY_APPLICATION = "APPLICATION";
  private String size;
  private String timestamp;

  public String getUrl() {
    return url;
  }

  public FileResource setUrl(String url) {
    this.url = url;
    return this;
  }

  public String getType() {
    return type;
  }

  public FileResource setType(String type) {
    this.type = type;
    return this;
  }

  public String getVisibility() {
    return visibility;
  }

  public FileResource setVisibility(String visibility) {
    this.visibility = visibility;
    return this;
  }

  public String getSize() {
    return size;
  }

  public FileResource setSize(final String size) {
    this.size = size;
    return this;
  }

  public String getTimestamp() {
    return timestamp;
  }

  public FileResource setTimestamp(String timestamp) {
    this.timestamp = timestamp;
    return this;
  }

  @Override
  public String toString() {
    return "FileResource{" +
        "url='" + url + '\'' +
        ", type='" + type + '\'' +
        ", visibility='" + visibility + '\'' +
        ", size=" + size +
        ", timestamp=" + timestamp +
        '}';
  }
}
