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
package org.apache.reef.tang.examples;

import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;

public class TimerV1 {

  private final int seconds;

  @Inject
  public TimerV1(@Parameter(Seconds.class) int seconds) {
    this.seconds = seconds;
  }

  public static void main(String[] args) throws BindException, InjectionException {
    Tang tang = Tang.Factory.getTang();
    JavaConfigurationBuilder cb = (JavaConfigurationBuilder) tang.newConfigurationBuilder();
    Configuration conf = cb.build();
    Injector injector = tang.newInjector(conf);
    TimerV1 timer = injector.getInstance(TimerV1.class);

    try {
      System.out.println("Tick...");
      timer.sleep();
      System.out.println("Tock.");
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void sleep() throws InterruptedException {
    java.lang.Thread.sleep(seconds * 1000);
  }

  @NamedParameter(default_value = "10",
      doc = "Number of seconds to sleep", short_name = "sec")
  class Seconds implements Name<Integer> {
  }
}
