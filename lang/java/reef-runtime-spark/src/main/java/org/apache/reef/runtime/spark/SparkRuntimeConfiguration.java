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
package org.apache.reef.runtime.spark;

import org.apache.spark.sql.internal.SQLConf;

import java.util.NoSuchElementException;

/**
 * Runtime configuration interface for Spark.
 * <p>
 * Options set here are automatically propagated to the Hadoop configuration during I/O.
 */
public class SparkRuntimeConfiguration {

  /** A handle to the sql configuration. **/
  private SQLConf sqlConf = new SQLConf();


  /**
   * Sets the given Spark runtime configuration property.
   *
   * @param key   the key to whom we store a value
   * @param value the value for the key
   */
  public void set(final String key, final String value) {
    requireNonStaticConf(key);
    sqlConf.setConfString(key, value);
  }


  /**
   * Sets the given Spark runtime configuration property.
   *
   * @param key   the key which we are processing
   * @param value the value which we are setting
   */
  public void set(final String key, final Boolean value) {
    requireNonStaticConf(key);
  }


  /**
   * Sets the given Spark runtime configuration property.
   *
   * @param key   the key which we are processing
   * @param value the value which we are setting
   */
  public void set(final String key, final Long value) {
    requireNonStaticConf(key);
  }


  /**
   * Returns the value of Spark runtime configuration property for the given key.
   *
   * @param key the key which we are processing
   * @throws java.util.NoSuchElementException if the key is not set and does not have a default
   *                                          value
   */
  public String get(final String key) throws NoSuchElementException {
    return sqlConf.getConfString(key);
  }


  /**
   * Returns the value of Spark runtime configuration property for the given key.
   *
   * @param key the key which we are processing
   * @throws java.util.NoSuchElementException if the key is not set and does not have a default
   *                                          value
   */
  public String get(final String key, final String def) {
    return sqlConf.getConfString(key, def);
  }


  /**
   * Given a key check the static config to determine whether or not to throw an exception.
   *
   * @param key
   */
  private void requireNonStaticConf(final String key) {
    //empty for now need to figure out how to check for keys
  }
}
