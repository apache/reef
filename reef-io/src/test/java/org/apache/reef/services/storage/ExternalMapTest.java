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
package org.apache.reef.services.storage;

import org.apache.reef.io.ExternalMap;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.io.storage.ram.CodecRamMap;
import org.apache.reef.io.storage.ram.RamMap;
import org.apache.reef.io.storage.ram.RamStorageService;
import org.apache.reef.io.storage.util.IntegerCodec;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;


public class ExternalMapTest {
  @Test
  public void testCodecRamMap() {
    RamStorageService ramStore = new RamStorageService();
    Codec<Integer> c = new IntegerCodec();
    ExternalMap<Integer> m = new CodecRamMap<>(ramStore, c);
    genericTest(m);
  }

  @Test
  public void testRamMap() {
    RamStorageService ramStore = new RamStorageService();
    ExternalMap<Integer> m = new RamMap<>(ramStore);
    genericTest(m);
  }


  void genericTest(ExternalMap<Integer> m) {
    m.put("foo", 42);
    Map<String, Integer> smallMap = new HashMap<>();
    smallMap.put("bar", 43);
    smallMap.put("baz", 44);

    m.putAll(smallMap);

    Assert.assertEquals(44, (int) m.get("baz"));
    Assert.assertEquals(43, (int) m.get("bar"));
    Assert.assertEquals(42, (int) m.get("foo"));
    Assert.assertNull(m.get("quuz"));

    Assert.assertTrue(m.containsKey("bar"));
    Assert.assertFalse(m.containsKey("quuz"));

    Set<String> barBaz = new HashSet<>();
    barBaz.add("bar");
    barBaz.add("baz");
    barBaz.add("quuz");

    Iterable<Map.Entry<CharSequence, Integer>> it = m.getAll(barBaz);

    Map<CharSequence, Integer> found = new TreeMap<>();

    for (Map.Entry<CharSequence, Integer> e : it) {
      found.put(e.getKey(), e.getValue());
    }
    Iterator<CharSequence> it2 = found.keySet().iterator();
    Assert.assertTrue(it2.hasNext());
    CharSequence s = it2.next();
    Assert.assertEquals(s, "bar");
    Assert.assertEquals((int) found.get(s), 43);
    Assert.assertTrue(it2.hasNext());
    s = it2.next();
    Assert.assertEquals(s, "baz");
    Assert.assertEquals((int) found.get(s), 44);
    Assert.assertFalse(it2.hasNext());

    Assert.assertEquals(44, (int) m.remove("baz"));
    Assert.assertFalse(m.containsKey("baz"));

  }

}
