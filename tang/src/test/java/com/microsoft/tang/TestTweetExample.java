package com.microsoft.tang;

import javax.inject.Inject;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationModuleBuilder;
import com.microsoft.tang.formats.RequiredParameter;

public class TestTweetExample {
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

    @NamedParameter()
    class PhoneNumber implements Name<Long> {}
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
  }

  public static final class TweetConfig extends ConfigurationModuleBuilder {
    public static final RequiredParameter<Long> PHONE_NUMBER = new RequiredParameter<>();
    static final ConfigurationModule CONF = new TweetConfig()
      .bindImplementation(TweetFactory.class, MockTweetFactory.class)
      .bindImplementation(SMS.class, MockSMS.class)
      .bindNamedParameter(Tweeter.PhoneNumber.class, PHONE_NUMBER)
      .build();
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  Tang tang;
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
}
