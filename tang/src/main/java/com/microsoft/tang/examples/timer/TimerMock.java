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
package com.microsoft.tang.examples.timer;

import javax.inject.Inject;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationModuleBuilder;
import com.microsoft.tang.formats.OptionalParameter;

public class TimerMock implements Timer {

  public static class TimerMockConf extends ConfigurationModuleBuilder {
    public static final OptionalParameter<Integer> MOCK_SLEEP_TIME = new OptionalParameter<>();
  }
  public static final ConfigurationModule CONF = new TimerMockConf()
    .bindImplementation(Timer.class, TimerMock.class)
    .bindNamedParameter(Timer.Seconds.class, TimerMockConf.MOCK_SLEEP_TIME)
    .build();
  
  private final int seconds;
  
  @Inject
  TimerMock(@Parameter(Timer.Seconds.class) int seconds) {
    if(seconds < 0) {
      throw new IllegalArgumentException("Cannot sleep for negative time!");
    }
    this.seconds = seconds; 
  }
  @Override
  public void sleep() {
    System.out.println("Would have slept for " + seconds + "sec.");
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
}
