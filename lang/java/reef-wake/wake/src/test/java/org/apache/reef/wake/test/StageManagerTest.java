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
package org.apache.reef.wake.test;

import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.ThreadPoolStage;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class StageManagerTest {

  final String logPrefix = "TEST ";
  @Rule
  public TestName name = new TestName();

  @Test
  public void testStageManager() throws InterruptedException {
    for (int i = 0; i < 5; ++i) {
      new ThreadPoolStage<Void>(new TestEventHandler(), 3);
    }
  }

}

class TestEventHandler implements EventHandler<Void> {

  @Override
  public void onNext(Void value) {
  }

}
