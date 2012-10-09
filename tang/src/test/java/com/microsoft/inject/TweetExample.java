package com.microsoft.inject;

import static org.junit.Assert.*;

import javax.inject.Inject;
import javax.inject.Named;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TweetExample {
    private interface TweetFactory {
        public String getTweet();
    }

    private interface SMS {
        public void sendSMS(String msg, long phoneNumber);
    }

    private class MockTweetFactory implements TweetFactory {
        @Override
        public String getTweet() {
            return "@tw #bbq bbqftw!!! gopher://vuwed.wefd/bbqftw!";
        }
    }

    private class MockSMS implements SMS {
        @Override
        public void sendSMS(String msg, long phoneNumber) {
            if (phoneNumber != 867 - 5309) {
                throw new IllegalArgumentException("Unknown recipient");
            }
            // success!
        }
    }

    private class Tweeter {
        final TweetFactory tw;
        final SMS sms;
        final long phoneNumber;

        @Inject
        public Tweeter(TweetFactory tw, SMS sms,
                @Named("phone-number") long phoneNumber) {
            this.tw = tw;
            this.sms = sms;
            this.phoneNumber = phoneNumber;
        }

        public void sendMessage() {
            sms.sendSMS(tw.getTweet(), phoneNumber);
        }
    }

    private Namespace ns;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        ns = new Namespace();
        ns.registerClass(MockTweetFactory.class);
        ns.registerClass(MockSMS.class);
        // Pull in unknown dependencies (eg: TweetFactory) automagically.
    }

    @After
    public void tearDown() throws Exception {
        ns = null;
    }

    @Test
    public void test() {
        Tang t = new Tang(ns);
        t.setDefaultImpl(TweetFactory.class, MockTweetFactory.class);
        t.setDefaultImpl(SMS.class, MockSMS.class);
        t.setNamedParameter("phone-number", 867-5309);
        Tweeter tw = Tang.getInstance(Tweeter.class);
        tw.sendMessage();
    }

}
