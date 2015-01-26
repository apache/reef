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

import org.apache.reef.io.storage.MergingIterator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;

public class MergingIteratorTest {

  @Test
  public void testMergingIterator() {
    Comparator<Integer> cmp = new Comparator<Integer>() {

      @Override
      public int compare(Integer o1, Integer o2) {
        return Integer.compare(o1, o2);
      }
    };
    @SuppressWarnings("unchecked")
    Iterator<Integer>[] its = new Iterator[]{
        Arrays.asList(new Integer[]{1, 4, 7, 10}).iterator(),
        Arrays.asList(new Integer[]{2, 5, 8, 11}).iterator(),
        Arrays.asList(new Integer[]{3, 6, 9, 12}).iterator()
    };
    MergingIterator<Integer> merge = new MergingIterator<Integer>(cmp, its);
    int i = 1;
    while (merge.hasNext()) {
      Assert.assertEquals(i, (int) merge.next());
      i++;
    }
    Assert.assertEquals(13, i);
  }
}
