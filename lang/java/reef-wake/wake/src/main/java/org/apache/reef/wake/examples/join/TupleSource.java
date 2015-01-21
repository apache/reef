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
package org.apache.reef.wake.examples.join;

import org.apache.reef.wake.Stage;
import org.apache.reef.wake.rx.Observer;
import org.apache.reef.wake.rx.StaticObservable;

public class TupleSource implements StaticObservable, Stage {
  final Thread[] threads;
  final Observer<TupleEvent> out;

  public TupleSource(final Observer<TupleEvent> out, final int max, final int numThreads, final boolean evenOnly) {
    this.out = out;
    threads = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      final int threadid = i;
      threads[i] = new Thread(new Runnable() {
        @Override
        public void run() {
          for (int i = 0; i < max / ((evenOnly ? 2 : 1) * numThreads); i++) {
            int j = i * numThreads + threadid;
            if (evenOnly) {
              j *= 2;
            }
            out.onNext(new TupleEvent(j, j + ""));
          }
        }

      });
      threads[i].start();
    }
  }

  @Override
  public void close() throws Exception {
    for (Thread t : threads) {
      t.join();
    }
    out.onCompleted();
  }
}
