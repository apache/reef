/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.wake.test;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.microsoft.wake.EventHandler;
import com.microsoft.wake.impl.ThreadPoolStage;

public class StageManagerTest {

  @Rule public TestName name = new TestName();

  final String logPrefix = "TEST ";
  
  @Test
  public void testStageManager() throws InterruptedException {
    for (int i=0; i<5; ++i) {
      new ThreadPoolStage<Void>(new TestEventHandler(), 3);
    }
  }
  
}

class TestEventHandler implements EventHandler<Void> {

  @Override
  public void onNext(Void value) {
  }
  
}
