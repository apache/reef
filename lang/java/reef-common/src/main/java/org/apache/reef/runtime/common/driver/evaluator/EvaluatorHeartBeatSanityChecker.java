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
package org.apache.reef.runtime.common.driver.evaluator;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Sanity checks for evaluator heartbeats.
 */
@DriverSide
@Private
final class EvaluatorHeartBeatSanityChecker {
  private static final Logger LOG = Logger.getLogger(EvaluatorHeartBeatSanityChecker.class.getName());
  private final Map<String, Long> knownTimeStamps = new HashMap<>(); // guarded by this

  @Inject
  EvaluatorHeartBeatSanityChecker() {
  }

  final synchronized void check(final String id, final long timeStamp) {
    if (knownTimeStamps.containsKey(id)) {
      final long oldTimeStamp = this.knownTimeStamps.get(id);
      LOG.log(Level.FINEST, "TIMESTAMP CHECKER: id [ " + id + " ], old timestamp [ " + oldTimeStamp + " ], new timestamp [ " + timeStamp + " ]");
      if (oldTimeStamp > timeStamp) {
        final String msg = "Received an old heartbeat with timestamp `" + timeStamp +
            "` while earlier receiving one with timestamp `" + oldTimeStamp + "`";
        LOG.log(Level.SEVERE, msg);
        throw new RuntimeException(msg);
      }
    }
    knownTimeStamps.put(id, timeStamp);
  }

}
