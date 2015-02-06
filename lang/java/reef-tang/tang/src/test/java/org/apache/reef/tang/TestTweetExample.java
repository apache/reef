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
package org.apache.reef.tang;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.RequiredParameter;
import org.junit.*;

import javax.inject.Inject;

public class TestTweetExample {
  Tang tang;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Before
  public void setUp() throws Exception {
    tang = Tang.Factory.getTang();
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void test() throws Exception {
    Tweeter tw = (Tweeter) tang.newInjector(TweetConfig.CONF.set(TweetConfig.PHONE_NUMBER, new Long(867 - 5309)).build()).getInstance(Tweeter.class);
    tw.sendMessage();
  }

  static interface TweetFactory {
    public String getTweet();
  }

  static interface SMS {
    public void sendSMS(String msg, long phoneNumber);
  }

  static class MockTweetFactory implements TweetFactory {
    @Inject
    public MockTweetFactory() {
    }

    @Override
    public String getTweet() {
      return "@tw #bbq bbqftw!!! gopher://vuwed.wefd/bbqftw!";
    }
  }

  static class MockSMS implements SMS {
    @Inject
    public MockSMS() {
    }

    @Override
    public void sendSMS(String msg, long phoneNumber) {
      if (phoneNumber != 867 - 5309) {
        throw new IllegalArgumentException("Unknown recipient");
      }
      // success!
    }
  }

  static class Tweeter {
    final TweetFactory tw;
    final SMS sms;
    final long phoneNumber;

    @Inject
    public Tweeter(TweetFactory tw, SMS sms,
                   @Parameter(PhoneNumber.class) long phoneNumber) {
      this.tw = tw;
      this.sms = sms;
      this.phoneNumber = phoneNumber;
    }

    public void sendMessage() {
      sms.sendSMS(tw.getTweet(), phoneNumber);
    }

    @NamedParameter()
    class PhoneNumber implements Name<Long> {
    }
  }

  public static final class TweetConfig extends ConfigurationModuleBuilder {
    public static final RequiredParameter<Long> PHONE_NUMBER = new RequiredParameter<>();
    static final ConfigurationModule CONF = new TweetConfig()
        .bindImplementation(TweetFactory.class, MockTweetFactory.class)
        .bindImplementation(SMS.class, MockSMS.class)
        .bindNamedParameter(Tweeter.PhoneNumber.class, PHONE_NUMBER)
        .build();
  }
}
