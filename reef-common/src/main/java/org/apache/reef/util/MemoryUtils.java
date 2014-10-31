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
package org.apache.reef.util;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.util.List;

/**
 * Utility class to report current and peak memory
 * usage. Structured to be used while logging. Is
 * useful for debugging memory issues
 */
public final class MemoryUtils {

  private static final int MBs = 1024 * 1024;

  private MemoryUtils() {
  }

  public static String memPoolNames() {
    final List<MemoryPoolMXBean> memoryPoolMXBeans = ManagementFactory.getMemoryPoolMXBeans();
    final StringBuilder output = new StringBuilder();
    for (final MemoryPoolMXBean bean : memoryPoolMXBeans) {
      output.append(bean.getName());
      output.append(",");
    }
    output.deleteCharAt(output.length() - 1);
    return output.toString();
  }

  public static long currentEdenMemoryUsageMB() {
    return currentMemoryUsage("eden");
  }

  public static long currentOldMemoryUsageMB() {
    return currentMemoryUsage("old");
  }

  public static long currentPermMemoryUsageMB() {
    return currentMemoryUsage("perm");
  }

  private static long currentMemoryUsage(final String name) {
    final List<MemoryPoolMXBean> memoryPoolMXBeans = ManagementFactory.getMemoryPoolMXBeans();
    for (final MemoryPoolMXBean bean : memoryPoolMXBeans) {
      if (bean.getName().toLowerCase().indexOf(name) != -1) {
        return bean.getUsage().getUsed() / MBs;
      }
    }
    return 0;
  }

  public static long peakEdenMemoryUsageMB() {
    return peakMemoryUsage("eden");
  }

  public static long peakOldMemoryUsageMB() {
    return peakMemoryUsage("old");
  }

  public static long peakPermMemoryUsageMB() {
    return peakMemoryUsage("perm");
  }

  private static long peakMemoryUsage(final String name) {
    final List<MemoryPoolMXBean> memoryPoolMXBeans = ManagementFactory.getMemoryPoolMXBeans();
    for (final MemoryPoolMXBean bean : memoryPoolMXBeans) {
      if (bean.getName().toLowerCase().indexOf(name) != -1) {
        return bean.getPeakUsage().getUsed() / MBs;
      }
    }
    return 0;
  }

  public static void resetPeakUsage() {
    final List<MemoryPoolMXBean> memoryPoolMXBeans = ManagementFactory.getMemoryPoolMXBeans();
    for (final MemoryPoolMXBean memoryPoolMXBean : memoryPoolMXBeans) {
      memoryPoolMXBean.resetPeakUsage();
    }
  }

  public static void main(final String[] args) {
    System.out.println(memPoolNames());
    {
      final byte[] b = new byte[1 << 24];
      System.out.println(currentEdenMemoryUsageMB()
          + "," + currentOldMemoryUsageMB()
          + "," + currentPermMemoryUsageMB());
    }

    System.gc();
    System.out.println(currentEdenMemoryUsageMB()
        + "," + currentOldMemoryUsageMB()
        + "," + currentPermMemoryUsageMB());
    System.out.println(peakEdenMemoryUsageMB()
        + "," + peakOldMemoryUsageMB()
        + "," + peakPermMemoryUsageMB());
    resetPeakUsage();
    System.out.println(peakEdenMemoryUsageMB()
        + "," + peakOldMemoryUsageMB()
        + "," + peakPermMemoryUsageMB());
  }
}
