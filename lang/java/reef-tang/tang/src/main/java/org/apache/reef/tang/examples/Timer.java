/*
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
package org.apache.reef.tang.examples;

import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.tang.implementation.InjectionPlan;
import org.apache.reef.tang.implementation.java.InjectorImpl;

import javax.inject.Inject;

public class Timer {
  private final int seconds;

  @Inject
  public Timer(@Parameter(Seconds.class) final int seconds) {
    if (seconds < 0) {
      throw new IllegalArgumentException("Cannot sleep for negative time!");
    }
    this.seconds = seconds;
  }

  public static void main(final String[] args) throws Exception {
    final Tang tang = Tang.Factory.getTang();
    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    final CommandLine cl = new CommandLine(cb);
    cl.registerShortNameOfClass(Timer.Seconds.class);
    cl.processCommandLine(args);
    final Configuration conf = cb.build();
    System.out.println("start conf");
    final AvroConfigurationSerializer avroSerializer = new AvroConfigurationSerializer();
    System.out.println(avroSerializer.toString(conf));
    System.out.println("end conf");
    final InjectorImpl injector = (InjectorImpl) tang.newInjector(conf);
    final InjectionPlan<Timer> ip = injector.getInjectionPlan(Timer.class);
    System.out.println(ip.toPrettyString());
    System.out.println("Number of plans:" + ip.getNumAlternatives());
    final Timer timer = injector.getInstance(Timer.class);
    System.out.println("Tick...");
    timer.sleep();
    System.out.println("Tock.");
  }

  public void sleep() throws InterruptedException {
    java.lang.Thread.sleep(seconds * 1000);
  }

  @NamedParameter(default_value = "10",
      doc = "Number of seconds to sleep", short_name = "sec")
  class Seconds implements Name<Integer> {
  }
}
