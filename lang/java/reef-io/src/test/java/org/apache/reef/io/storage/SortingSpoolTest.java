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
package org.apache.reef.io.storage;

import org.apache.reef.exception.evaluator.ServiceException;
import org.apache.reef.io.Accumulator;
import org.apache.reef.io.Spool;
import org.apache.reef.io.storage.ram.SortingRamSpool;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class SortingSpoolTest {

  @Test
  public void testRamSpool() throws ServiceException {
    genericTest(new SortingRamSpool<Integer>(), new Comparator<Integer>() {

      @Override
      public int compare(final Integer o1, final Integer o2) {
        return Integer.compare(o1, o2);
      }

    });
  }

  @Test
  public void testRamSpoolComparator() throws ServiceException {
    final Comparator<Integer> backwards = new Comparator<Integer>() {

      @Override
      public int compare(final Integer o1, final Integer o2) {
        return -1 * o1.compareTo(o2);
      }

    };
    genericTest(new SortingRamSpool<Integer>(backwards), backwards);
  }

  @Test(expected = IllegalStateException.class)
  public void testRamSpoolAddAfterClose() throws ServiceException {
    final Spool<Integer> s = new SortingRamSpool<>();
    genericAddAfterCloseTest(s);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testRamSpoolCantRemove() throws ServiceException {
    final Spool<Integer> s = new SortingRamSpool<>();
    genericCantRemove(s);
  }

  @Test(expected = IllegalStateException.class)
  public void testIteratorBeforeClose() throws ServiceException {
    final Spool<Integer> s = new SortingRamSpool<>();
    genericIteratorBeforeClose(s);
  }

  void genericTest(final Spool<Integer> s, final Comparator<Integer> comparator)
      throws ServiceException {
    final List<Integer> l = new ArrayList<Integer>();
    final Random r = new Random(42);
    while (l.size() < 100) {
      l.add(r.nextInt(75));
    }
    final Accumulator<Integer> a = s.accumulator();
    for (int i = 0; i < 100; i++) {
      a.add(l.get(i));
    }
    a.close();
    final List<Integer> m = new ArrayList<Integer>();
    for (final int i : s) {
      m.add(i);
    }
    final Integer[] sorted = l.toArray(new Integer[0]);
    Arrays.sort(sorted, 0, sorted.length, comparator);
    final Integer[] shouldBeSorted = m.toArray(new Integer[0]);
    Assert.assertArrayEquals(sorted, shouldBeSorted);
  }

  void genericAddAfterCloseTest(final Spool<?> s) throws ServiceException {
    final Accumulator<?> a = s.accumulator();
    a.close();
    a.add(null);
  }

  void genericCantRemove(final Spool<Integer> s) throws ServiceException {
    final Accumulator<Integer> a = s.accumulator();
    a.add(10);
    a.close();
    final Iterator<?> it = s.iterator();
    it.remove();
  }

  void genericIteratorBeforeClose(final Spool<Integer> s) throws ServiceException {
    final Accumulator<Integer> a = s.accumulator();
    a.add(10);
    s.iterator();
  }

}
