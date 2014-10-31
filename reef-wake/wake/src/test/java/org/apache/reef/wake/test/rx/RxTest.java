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
package org.apache.reef.wake.test.rx;

import org.apache.reef.wake.rx.Observer;
import org.apache.reef.wake.rx.RxStage;
import org.apache.reef.wake.rx.impl.RxThreadPoolStage;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;


public class RxTest {

  @Rule
  public TestName name = new TestName();

  @Test
  public void testRx() throws Exception {
    System.out.println(name.getMethodName());

    RxStage<TestEvent> stage = new RxThreadPoolStage<TestEvent>(new TestObserver("o1"), 1);

    int i = 0;
    try {
      for (i = 0; i < 20; ++i)
        stage.onNext(new TestEvent(i));
      stage.onCompleted();
    } catch (Exception e) {
      stage.onError(e);
    }

    stage.close();
  }

  class TestEvent {
    private int n;

    TestEvent(int n) {
      this.n = n;
    }

    int get() {
      return n;
    }
  }

  class TestObserver implements Observer<TestEvent> {

    private final String name;

    TestObserver(String name) {
      this.name = name;
    }

    @Override
    public void onNext(TestEvent value) {
      System.out.println(name + " Value: " + value + " " + value.get());
    }

    @Override
    public void onError(Exception error) {
      System.out.println(name + " Error: " + error);
    }

    @Override
    public void onCompleted() {
      System.out.println(name + " Completed");
    }
  }

  ;

}
