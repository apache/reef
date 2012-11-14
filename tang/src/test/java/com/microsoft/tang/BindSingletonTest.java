package com.microsoft.tang;

import static org.junit.Assert.assertTrue;

import java.io.File;

import javax.inject.Inject;

import org.junit.Test;

import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;

public class BindSingletonTest {

    public static Configuration roundtrip(final Configuration conf) {
        try {
            final File f = File.createTempFile("TANGConf-", ".conf");
            conf.writeConfigurationFile(f);
            final ConfigurationBuilder b = Tang.Factory.getTang().newConfigurationBuilder();
            b.addConfiguration(f);
            return b.build();
        } catch (final Exception e) {
            throw new RuntimeException("Unable to roundtrip a TANG Configuration.", e);
        }
    }

    public static class A {

        @Inject
        public A(){
            
        }
        @SuppressWarnings("static-method")
        public void print() {
            System.err.println("A");
        }
    }

    public static class B extends A {
        @Inject
        public B() {
            // intentionally blank
        }

        @Override
        public void print() {
            System.err.println("B");
        }
    }

    @Test
    public void testSingletonRoundTrip() throws BindException, InjectionException {

        final ConfigurationBuilder b = Tang.Factory.getTang().newConfigurationBuilder();
        b.bindSingletonImplementation(A.class, B.class);
        final Configuration src = b.build();

        final Configuration dest = roundtrip(src);
        final Injector i = Tang.Factory.getTang().newInjector(dest);
        final A a1 = i.getInstance(A.class);
        final A a2 = i.getInstance(A.class);

        assertTrue("Two singletons should be the same", a1 == a2);
        assertTrue("Both instances should be of class B", a1 instanceof B);
        assertTrue("Both instances should be of class B", a2 instanceof B);
    }

}
