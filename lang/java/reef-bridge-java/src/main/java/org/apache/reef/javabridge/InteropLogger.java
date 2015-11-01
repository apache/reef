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
package org.apache.reef.javabridge;

import org.apache.reef.annotations.audience.Interop;
import org.apache.reef.annotations.audience.Private;

import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Logger called from CLR code.
 */
@Private
@Interop(CppFiles = { "JavaClrBridge.cpp", "InteropLogger.h", "InteropLogger.cpp" })
public final class InteropLogger {
  private static final Logger LOG = Logger.getLogger("InteropLogger");
  private HashMap<Integer, Level> levelHashMap;

  {
    levelHashMap = new HashMap<>();
    levelHashMap.put(Level.OFF.intValue(), Level.OFF);
    levelHashMap.put(Level.SEVERE.intValue(), Level.SEVERE);
    levelHashMap.put(Level.WARNING.intValue(), Level.WARNING);
    levelHashMap.put(Level.INFO.intValue(), Level.INFO);

    levelHashMap.put(Level.CONFIG.intValue(), Level.CONFIG);
    levelHashMap.put(Level.FINE.intValue(), Level.FINE);
    levelHashMap.put(Level.FINER.intValue(), Level.FINER);

    levelHashMap.put(Level.FINEST.intValue(), Level.FINEST);
    levelHashMap.put(Level.ALL.intValue(), Level.ALL);
  }

  public void log(final int intLevel, final String message) {
    if (levelHashMap.containsKey(intLevel)) {
      final Level level = levelHashMap.get(intLevel);
      LOG.log(level, message);
    } else {
      LOG.log(Level.WARNING, "Level " + intLevel + " is not a valid Log level");
      LOG.log(Level.WARNING, message);
    }
  }
}
