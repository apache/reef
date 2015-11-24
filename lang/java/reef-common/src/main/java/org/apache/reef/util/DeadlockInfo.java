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
package org.apache.reef.util;

import javax.annotation.Nullable;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.*;

/**
 * Provides a view into deadlocked threads for logging or debugging purposes.
 * Backed by ThreadMXBean
 */
final class DeadlockInfo {
  private final ThreadMXBean mxBean;
  private final ThreadInfo[] deadlockedThreads;
  private final long[] deadlockedThreadsIDs;
  private static final ThreadInfo[] EMPTY_ARRAY = new ThreadInfo[0];
  private final Map<ThreadInfo, Map<StackTraceElement, List<MonitorInfo>>> monitorLockedElements;

  DeadlockInfo() {
    mxBean = ManagementFactory.getThreadMXBean();
    deadlockedThreadsIDs = mxBean.findDeadlockedThreads();

    deadlockedThreads = (null == deadlockedThreadsIDs)
                        ? EMPTY_ARRAY
                        : mxBean.getThreadInfo(deadlockedThreadsIDs, true, true);

    monitorLockedElements = new HashMap<>();
    for (final ThreadInfo threadInfo : deadlockedThreads) {
      monitorLockedElements.put(threadInfo, constructMonitorLockedElements(threadInfo));
    }
  }

  /**
   * @return A (potentially empty) array of deadlocked threads
   */
  public ThreadInfo[] getDeadlockedThreads() {
    return deadlockedThreads;
  }

  /**
   * Get a list of monitor locks that were acquired by this thread at this stack element.
   * @param threadInfo The thread that created the stack element
   * @param stackTraceElement The stack element
   * @return List of monitor locks that were acquired by this thread at this stack element
   * or an empty list if none were acquired
   */
  public List<MonitorInfo> getMonitorLockedElements(final ThreadInfo threadInfo,
                                                    final StackTraceElement stackTraceElement) {
    final Map<StackTraceElement, List<MonitorInfo>> elementMap = monitorLockedElements.get(threadInfo);
    if (null == elementMap) {
      return Collections.EMPTY_LIST;
    }

    final List<MonitorInfo> monitorList = elementMap.get(stackTraceElement);
    if (null == monitorList) {
      return Collections.EMPTY_LIST;
    }

    return monitorList;
  }

  /**
   * Get a string identifying the lock that this thread is waiting on.
   * @param threadInfo
   * @return A string identifying the lock that this thread is waiting on,
   * or null if the thread is not waiting on a lock
   */
  @Nullable
  public String getWaitingLockString(final ThreadInfo threadInfo) {
    if (null == threadInfo.getLockInfo()) {
      return null;
    } else {
      return threadInfo.getLockName() + " held by " + threadInfo.getLockOwnerName();
    }
  }

  private static Map<StackTraceElement, List<MonitorInfo>> constructMonitorLockedElements(final ThreadInfo threadInfo) {
    final Map<StackTraceElement, List<MonitorInfo>> monitorLockedElements = new HashMap<>();
    for (final MonitorInfo monitorInfo : threadInfo.getLockedMonitors()) {
      final List<MonitorInfo> monitorInfoList = monitorLockedElements.containsKey(monitorInfo.getLockedStackFrame()) ?
          monitorLockedElements.get(monitorInfo.getLockedStackFrame()) : new LinkedList<MonitorInfo>();
      monitorInfoList.add(monitorInfo);
      monitorLockedElements.put(monitorInfo.getLockedStackFrame(), monitorInfoList);
    }
    return monitorLockedElements;
  }
}
