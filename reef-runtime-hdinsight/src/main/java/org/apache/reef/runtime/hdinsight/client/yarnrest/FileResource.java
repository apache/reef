/**
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
package org.apache.reef.runtime.hdinsight.client.yarnrest;

public final class FileResource {

  public static final String TYPE_FILE = "FILE";
  public static final String TYPE_ARCHIVE = "ARCHIVE";

  public static final String VISIBILITY_APPLICATION = "APPLICATION";

  private String url;
  private String type;
  private String visibility;
  private String size;
  private String timestamp;

  public String getUrl() {
    return this.url;
  }

  public FileResource setUrl(final String url) {
    this.url = url;
    return this;
  }

  public String getType() {
    return this.type;
  }

  public FileResource setType(final String type) {
    this.type = type;
    return this;
  }

  public String getVisibility() {
    return this.visibility;
  }

  public FileResource setVisibility(final String visibility) {
    this.visibility = visibility;
    return this;
  }

  public String getSize() {
    return this.size;
  }

  public FileResource setSize(final String size) {
    this.size = size;
    return this;
  }

  public String getTimestamp() {
    return this.timestamp;
  }

  public FileResource setTimestamp(final String timestamp) {
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
