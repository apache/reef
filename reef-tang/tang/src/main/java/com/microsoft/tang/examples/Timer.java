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
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.formats.CommandLine;
import com.microsoft.tang.formats.ConfigurationFile;
import com.microsoft.tang.implementation.InjectionPlan;
import com.microsoft.tang.implementation.java.InjectorImpl;

public class Timer {
  @NamedParameter(default_value="10",
      doc="Number of seconds to sleep", short_name="sec")
  class Seconds implements Name<Integer> {}
  private final int seconds;

  @Inject
  public Timer(@Parameter(Seconds.class) int seconds) {
    if(seconds < 0) {
      throw new IllegalArgumentException("Cannot sleep for negative time!");
    }
    this.seconds = seconds;
  }

  public void sleep() throws InterruptedException {
    java.lang.Thread.sleep(seconds * 1000);
  }
  
  public static void main(String[] args) throws Exception {
    Tang tang = Tang.Factory.getTang();
    JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    CommandLine cl = new CommandLine(cb);
    cl.registerShortNameOfClass(Timer.Seconds.class);
    cl.processCommandLine(args);
    Configuration conf = cb.build();
    System.out.println("start conf");
    System.out.println(ConfigurationFile.toConfigurationString(conf));
    System.out.println("end conf");
    InjectorImpl injector = (InjectorImpl)tang.newInjector(conf);
    InjectionPlan<Timer> ip = injector.getInjectionPlan(Timer.class);
    System.out.println(ip.toPrettyString());
    System.out.println("Number of plans:" + ip.getNumAlternatives());
    Timer timer = injector.getInstance(Timer.class);
    System.out.println("Tick...");
    timer.sleep();
    System.out.println("Tock.");
  }
}
