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

import org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Utility map class that wraps a CountingMap.
 * in a ConcurrentMap
 * Equivalent to {@code Map<K,Map<V,Integer>>}
 */
public class ConcurrentCountingMap<K, V> {
  private static final Logger LOG = Logger.getLogger(ConcurrentCountingMap.class.getName());
  private final ConcurrentMap<K, CountingMap<V>> map = new ConcurrentHashMap<>();

  public boolean remove(final K key, final V value) {
    if (!map.containsKey(key)) {
      return false;
    }
    final boolean retVal = map.get(key).remove(value);
    if (map.get(key).isEmpty()) {
      map.remove(key);
    }
    return retVal;
  }

  public void add(final K key, final V value) {
    map.putIfAbsent(key, new CountingMap<V>());
    map.get(key).add(value);
  }

  public CountingMap<V> get(final K key) {
    return map.get(key);
  }

  public boolean isEmpty() {
    return map.isEmpty();
  }

  public boolean containsKey(final K key) {
    return map.containsKey(key);
  }

  public boolean contains(final K key, final V value) {
    if (!map.containsKey(key)) {
      return false;
    }
    return map.get(key).containsKey(value);
  }

  public boolean notContains(final V value) {
    for (final CountingMap<V> innerMap : map.values()) {
      if (innerMap.containsKey(value)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    return map.toString();
  }

  public void clear() {
    for (final CountingMap<V> value : map.values()) {
      value.clear();
    }
    map.clear();
  }

  public static void main(final String[] args) {
    final ConcurrentCountingMap<ReefNetworkGroupCommProtos.GroupCommMessage.Type, String> strMap =
        new ConcurrentCountingMap<>();
    LOG.log(Level.INFO, "OUT: {0}", strMap.isEmpty());
    strMap.add(ReefNetworkGroupCommProtos.GroupCommMessage.Type.ChildAdd, "ST0");
    LOG.log(Level.INFO, "OUT: {0}", strMap);
    strMap.add(ReefNetworkGroupCommProtos.GroupCommMessage.Type.ChildAdd, "ST1");
    LOG.log(Level.INFO, "OUT: {0}", strMap);
    strMap.add(ReefNetworkGroupCommProtos.GroupCommMessage.Type.ChildDead, "ST0");
    LOG.log(Level.INFO, "OUT: {0}", strMap);
    strMap.add(ReefNetworkGroupCommProtos.GroupCommMessage.Type.ChildDead, "ST1");
    LOG.log(Level.INFO, "OUT: {0}", strMap);
    strMap.add(ReefNetworkGroupCommProtos.GroupCommMessage.Type.ChildAdd, "ST2");
    LOG.log(Level.INFO, "OUT: {0}", strMap);
    strMap.add(ReefNetworkGroupCommProtos.GroupCommMessage.Type.ChildAdd, "ST3");
    LOG.log(Level.INFO, "OUT: {0}", strMap);
    strMap.add(ReefNetworkGroupCommProtos.GroupCommMessage.Type.ChildAdd, "ST0");
    LOG.log(Level.INFO, "OUT: {0}", strMap);
    strMap.add(ReefNetworkGroupCommProtos.GroupCommMessage.Type.ChildAdd, "ST1");
    LOG.log(Level.INFO, "OUT: {0}", strMap);
    strMap.remove(ReefNetworkGroupCommProtos.GroupCommMessage.Type.ChildAdd, "ST2");
    LOG.log(Level.INFO, "OUT: {0}", strMap);
    strMap.remove(ReefNetworkGroupCommProtos.GroupCommMessage.Type.ChildAdd, "ST3");
    LOG.log(Level.INFO, "OUT: {0}", strMap);
    strMap.remove(ReefNetworkGroupCommProtos.GroupCommMessage.Type.ChildAdd, "ST0");
    LOG.log(Level.INFO, "OUT: {0}", strMap);
    LOG.log(Level.INFO, "OUT: {0}", strMap.get(ReefNetworkGroupCommProtos.GroupCommMessage.Type.ChildAdd));
    LOG.log(Level.INFO, "OUT: {0}", strMap.get(ReefNetworkGroupCommProtos.GroupCommMessage.Type.ChildDead));
    strMap.remove(ReefNetworkGroupCommProtos.GroupCommMessage.Type.ChildAdd, "ST1");
    LOG.log(Level.INFO, "OUT: {0}", strMap);
    strMap.remove(ReefNetworkGroupCommProtos.GroupCommMessage.Type.ChildAdd, "ST1");
    LOG.log(Level.INFO, "OUT: {0}", strMap);
    LOG.log(Level.INFO, "OUT: {0}", strMap.containsKey(ReefNetworkGroupCommProtos.GroupCommMessage.Type.ChildAdd));
    LOG.log(Level.INFO, "OUT: {0}", strMap.containsKey(ReefNetworkGroupCommProtos.GroupCommMessage.Type.ChildDead));
    LOG.log(Level.INFO, "OUT: {0}", strMap.contains(ReefNetworkGroupCommProtos.GroupCommMessage.Type.ChildAdd, "ST0"));
    LOG.log(Level.INFO, "OUT: {0}", strMap.contains(ReefNetworkGroupCommProtos.GroupCommMessage.Type.ChildAdd, "ST2"));
    strMap.remove(ReefNetworkGroupCommProtos.GroupCommMessage.Type.ChildAdd, "ST0");
    LOG.log(Level.INFO, "OUT: {0}", strMap);
    LOG.log(Level.INFO, "OUT: {0}", strMap.containsKey(ReefNetworkGroupCommProtos.GroupCommMessage.Type.ChildAdd));
    LOG.log(Level.INFO, "OUT: {0}", strMap.isEmpty());
  }
}
