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
package org.apache.reef.tang.examples.timer;

import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalParameter;

import javax.inject.Inject;

public class TimerMock implements Timer {

  public static final ConfigurationModule CONF = new TimerMockConf()
      .bindImplementation(Timer.class, TimerMock.class)
      .bindNamedParameter(Timer.Seconds.class, TimerMockConf.MOCK_SLEEP_TIME)
      .build();
  private final int seconds;

  @Inject
  TimerMock(@Parameter(Timer.Seconds.class) int seconds) {
    if (seconds < 0) {
      throw new IllegalArgumentException("Cannot sleep for negative time!");
    }
    this.seconds = seconds;
  }

  public static void main(String[] args) throws BindException, InjectionException, Exception {
    Configuration c = TimerMock.CONF
        .set(TimerMockConf.MOCK_SLEEP_TIME, 1)
        .build();
    Timer t = Tang.Factory.getTang().newInjector(c).getInstance(Timer.class);
    System.out.println("Tick...");
    t.sleep();
    System.out.println("...tock.");
  }

  @Override
  public void sleep() {
    System.out.println("Would have slept for " + seconds + "sec.");
  }

  public static class TimerMockConf extends ConfigurationModuleBuilder {
    public static final OptionalParameter<Integer> MOCK_SLEEP_TIME = new OptionalParameter<>();
  }
}
