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

/**
 * An Entry in the Environment field of an ApplicationSubmission
 */
public final class EnvironmentEntry {

  private String key;
  private String value;

  public EnvironmentEntry(final String key, final String value) {
    this.key = key;
    this.value = value;
  }

  public String getKey() {
    return this.key;
  }

  public void setKey(final String key) {
    this.key = key;
  }

  public String getValue() {
    return this.value;
  }

  public void setValue(final String value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return "EnvironmentEntry{" +
        "key='" + this.key + '\'' +
        ", value='" + this.value + '\'' +
        '}';
  }

  @Override
  public boolean equals(final Object o) {

    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    final EnvironmentEntry that = (EnvironmentEntry) o;

    return (this.key == that.key || (this.key != null && this.key.equals(that.key)))
        && (this.value == that.value || (this.value != null && this.value.equals(that.value)));
  }

  @Override
  public int hashCode() {
    int result = this.key != null ? this.key.hashCode() : 0;
    result = 31 * result + (this.value != null ? this.value.hashCode() : 0);
    return result;
  }
}
