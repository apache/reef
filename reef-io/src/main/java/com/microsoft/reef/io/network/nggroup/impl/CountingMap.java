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
package com.microsoft.reef.io.network.nggroup.impl;

import java.util.HashMap;
import java.util.Map;

class CountingMap<L> {
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
    int cnt = (map.containsKey(value)) ? map.get(value) : 0;
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
    System.out.println(strMap);
    strMap.add("World");
    System.out.println(strMap);
    strMap.add("Hello");
    System.out.println(strMap);
    strMap.add("Hello");
    System.out.println(strMap);
    strMap.add("World!");
    System.out.println(strMap);
    strMap.remove("Hello");
    System.out.println(strMap);
    strMap.remove("World");
    System.out.println(strMap);
  }
}