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
package com.microsoft.tang.examples;

import javax.inject.Inject;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Injector;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;

public class TimerV1 {

  @NamedParameter(default_value="10",
      doc="Number of seconds to sleep", short_name="sec")
  class Seconds implements Name<Integer> {}
  private final int seconds;

  @Inject
  public TimerV1(@Parameter(Seconds.class) int seconds) {
    this.seconds = seconds;
  }

  public void sleep() throws InterruptedException {
    java.lang.Thread.sleep(seconds * 1000);
  }
  
  public static void main(String[] args) throws BindException, InjectionException {
    Tang tang = Tang.Factory.getTang();
    JavaConfigurationBuilder cb = (JavaConfigurationBuilder)tang.newConfigurationBuilder();
    Configuration conf = cb.build();
    Injector injector = tang.newInjector(conf);
    TimerV1 timer = injector.getInstance(TimerV1.class);
    
    try {
      System.out.println("Tick...");
      timer.sleep();
      System.out.println("Tock.");
    } catch(InterruptedException e) {
      e.printStackTrace();
    }
  }
}
