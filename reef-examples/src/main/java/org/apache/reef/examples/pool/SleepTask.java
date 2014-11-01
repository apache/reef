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
package org.apache.reef.examples.pool;

import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Sleep for delay seconds and quit.
 */
public class SleepTask implements Task {

  /**
   * Standard java logger.
   */
  private static final Logger LOG = Logger.getLogger(SleepTask.class.getName());

  /**
   * Number of milliseconds to sleep.
   */
  private final int delay;

  /**
   * Task constructor. Parameters are injected automatically by TANG.
   *
   * @param delay number of seconds to sleep.
   */
  @Inject
  private SleepTask(final @Parameter(Launch.Delay.class) Integer delay) {
    this.delay = delay * 1000;
  }

  /**
   * Sleep for delay milliseconds and return.
   *
   * @param memento ignored.
   * @return null.
   */
  @Override
  public byte[] call(final byte[] memento) {
    LOG.log(Level.FINE, "Task started: sleep for: {0} msec.", this.delay);
    final long ts = System.currentTimeMillis();
    for (long period = this.delay; period > 0; period -= System.currentTimeMillis() - ts) {
      try {
        Thread.sleep(period);
      } catch (final InterruptedException ex) {
        LOG.log(Level.FINEST, "Interrupted: {0}", ex);
      }
    }
    LOG.log(Level.FINE, "Task finished after {0} msec.", System.currentTimeMillis() - ts);
    return null;
  }
}
