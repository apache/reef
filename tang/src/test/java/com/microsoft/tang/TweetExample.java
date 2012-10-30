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
import com.microsoft.tang.implementation.ConfigurationBuilderImpl;
import com.microsoft.tang.implementation.InjectorImpl;


public class TweetExample {
  private static interface TweetFactory {
    public String getTweet();
  }

  private static interface SMS {
    public void sendSMS(String msg, long phoneNumber);
  }

  private static class MockTweetFactory implements TweetFactory {
    @Inject
    public MockTweetFactory() {
    }

    @Override
    public String getTweet() {
      return "@tw #bbq bbqftw!!! gopher://vuwed.wefd/bbqftw!";
    }
  }

  private static class MockSMS implements SMS {
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

  private static class Tweeter {
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
    ConfigurationBuilderImpl t = (ConfigurationBuilderImpl)tang.newConfigurationBuilder();
    t.register(Tweeter.class);
    t.bindImplementation(TweetFactory.class, MockTweetFactory.class);
    t.bindImplementation(SMS.class, MockSMS.class);
    t.bindNamedParameter(Tweeter.PhoneNumber.class, new Long(867 - 5309).toString());
    Tweeter tw = (Tweeter) new InjectorImpl(t.build()).getInstance(Tweeter.class);
    tw.sendMessage();
  }

}
