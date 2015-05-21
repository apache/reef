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
package org.apache.reef.examples.network;

import org.apache.reef.io.network.exception.NetworkRuntimeException;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Monitor for waiting message from remote nodes.
 */
public final class Monitor {

  private final AtomicBoolean finished;

  @Inject
  public Monitor(){
    finished = new AtomicBoolean(false);
  }

  public void monitorWait() {
    synchronized (this) {
      while (!finished.get())
        try {
          this.wait();
        } catch (InterruptedException e) {
          throw new NetworkRuntimeException(e);
        }
    }
  }

  public void monitorNotify() {
    synchronized (this) {
      finished.compareAndSet(false, true);
      this.notifyAll();
    }
  }
}
