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
package org.apache.reef.io.network.group.impl.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Utility class to provide a map that allows to
 * add multiple keys and automatically
 * incrementing the count on each add
 * decrementing the count on each remove
 * and removing key on count==0.
 */
public class CountingMap<L> {
  private static final Logger LOG = Logger.getLogger(CountingMap.class.getName());
  private final Map<L, Integer> map = new HashMap<>();

  public boolean containsKey(final L value) {
    return map.containsKey(value);
  }

  public int get(final L value) {
    if (!containsKey(value)) {
      return 0;
    }
    return map.get(value);
  }

  public boolean isEmpty() {
    return map.isEmpty();
  }

  public void clear() {
    map.clear();
  }

  public void add(final L value) {
    int cnt = map.containsKey(value) ? map.get(value) : 0;
    map.put(value, ++cnt);
  }

  public boolean remove(final L value) {
    if (!map.containsKey(value)) {
      return false;
    }
    int cnt = map.get(value);
    --cnt;
    if (cnt == 0) {
      map.remove(value);
    } else {
      map.put(value, cnt);
    }
    return true;
  }

  @Override
  public String toString() {
    return map.toString();
  }

  public static void main(final String[] args) {
    final CountingMap<String> strMap = new CountingMap<>();
    strMap.add("Hello");
    LOG.log(Level.INFO, "OUT: {0}", strMap);
    strMap.add("World");
    LOG.log(Level.INFO, "OUT: {0}", strMap);
    strMap.add("Hello");
    LOG.log(Level.INFO, "OUT: {0}", strMap);
    strMap.add("Hello");
    LOG.log(Level.INFO, "OUT: {0}", strMap);
    strMap.add("World!");
    LOG.log(Level.INFO, "OUT: {0}", strMap);
    strMap.remove("Hello");
    LOG.log(Level.INFO, "OUT: {0}", strMap);
    strMap.remove("World");
    LOG.log(Level.INFO, "OUT: {0}", strMap);
  }
}
