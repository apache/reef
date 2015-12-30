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
package org.apache.reef.runtime.hdinsight.client.yarnrest;

import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.StringWriter;

/**
 * An Entry with String Key and String Value in the Environment field
 * and the ApplicationAcls field of an ApplicationSubmission.
 * For detail information, please refer to
 * https://hadoop.apache.org/docs/r2.6.0/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html
 */
public final class StringEntry {

  private static final String STRING_ENTRY = "stringEntry";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private String key;
  private String value;

  public StringEntry(final String key, final String value) {
    this.key = key;
    this.value = value;
  }

  @JsonProperty(Constants.KEY)
  public String getKey() {
    return this.key;
  }

  public void setKey(final String key) {
    this.key = key;
  }

  @JsonProperty(Constants.VALUE)
  public String getValue() {
    return this.value;
  }

  public void setValue(final String value) {
    this.value = value;
  }

  @Override
  public String toString() {
    final StringWriter writer = new StringWriter();
    final String objectString;
    try {
      OBJECT_MAPPER.writeValue(writer, this);
      objectString = writer.toString();
    } catch (final IOException e) {
      return null;
    }

    return STRING_ENTRY + objectString;
  }

  @Override
  public boolean equals(final Object o) {

    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final StringEntry that = (StringEntry) o;

    return (this.key == null && that.key == null || this.key != null && this.key.equals(that.key))
        && (this.value == null && that.value == null || this.value != null && this.value.equals(that.value));
  }

  @Override
  public int hashCode() {
    int result = this.key != null ? this.key.hashCode() : 0;
    result = 31 * result + (this.value != null ? this.value.hashCode() : 0);
    return result;
  }
}
