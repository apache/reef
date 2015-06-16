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

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for ScatterSender.
 */
public final class ScatterHelper {

  /**
   * Should not be instantiated.
   */
  private ScatterHelper() {
  }

  /**
   * Uniformly distribute a number of elements across a number of Tasks and return a list of counts.
   * If uniform distribution is impossible, then some Tasks will receive one
   * more element than others. The sequence of the number of elements for each
   * Task is non-increasing.
   *
   * @param elementCount number of elements to distribute
   * @param taskCount number of Tasks that receive elements
   * @return list of counts specifying how many elements each Task should receive
   */
  public static List<Integer> getUniformCounts(final int elementCount, final int taskCount) {
    final int quotient = elementCount / taskCount;
    final int remainder = elementCount % taskCount;

    final List<Integer> retList = new ArrayList<>();
    for (int taskIndex = 0; taskIndex < taskCount; taskIndex++) {
      if (taskIndex < remainder) {
        retList.add(quotient + 1);
      } else {
        retList.add(quotient);
      }
    }
    return retList;
  }
}
