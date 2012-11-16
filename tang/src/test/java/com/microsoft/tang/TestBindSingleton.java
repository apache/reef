package com.microsoft.tang;

import static org.junit.Assert.assertTrue;

import javax.inject.Inject;

import org.junit.Test;

import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;

public class TestBindSingleton {

    public static class A {
        @Inject
        public A() {
            // Intentionally blank
        }

    }

    public static class B extends A {
        @Inject
        public B() {
            // intentionally blank
        }
    }

    @SuppressWarnings("static-method")
    @Test
    public void testSingletonRoundTrip() throws BindException, InjectionException {

        final ConfigurationBuilder b = Tang.Factory.getTang().newConfigurationBuilder();
        b.bindSingletonImplementation(A.class, B.class);
        final Configuration src = b.build();

        final Configuration dest = Utils.roundtrip(src);
        final Injector i = Tang.Factory.getTang().newInjector(dest);
        final A a1 = i.getInstance(A.class);
        final A a2 = i.getInstance(A.class);

        assertTrue("Two singletons should be the same", a1 == a2);
        assertTrue("Both instances should be of class B", a1 instanceof B);
        assertTrue("Both instances should be of class B", a2 instanceof B);

        final Injector injector2 = Tang.Factory.getTang().newInjector(src);
        final A a3 = injector2.getInstance(A.class);
        assertTrue("Two different injectors should return two different singletons", a3 != a1);

        final Injector injector3 = injector2.createChildInjector();
        final A a4 = injector3.getInstance(A.class);
        assertTrue("Child Injectors should return the same singletons as their parents", a3 == a4);
    }

    @Test
    public void testLateBoundVolatileInstanceWithSingletonX() throws BindException, InjectionException {
        Tang tang = Tang.Factory.getTang();
        ConfigurationBuilder cb = tang.newConfigurationBuilder();
        cb.bindSingletonImplementation(LateBoundVolatile.A.class, LateBoundVolatile.B.class);
        final Injector i = tang.newInjector(cb.build());
        i.bindVolatileInstance(LateBoundVolatile.C.class, new LateBoundVolatile.C());
        i.getInstance(LateBoundVolatile.A.class);
    }

    @Test
    public void testLateBoundVolatileInstanceWithSingletonY() throws BindException, InjectionException {
        Tang tang = Tang.Factory.getTang();
        ConfigurationBuilder cb = tang.newConfigurationBuilder();
        cb.bindSingleton(LateBoundVolatile.C.class);
        Injector i = tang.newInjector(cb.build());
        i.bindVolatileInstance(LateBoundVolatile.C.class, new LateBoundVolatile.C());
        i.getInstance(LateBoundVolatile.C.class);
    }

    @Test
    public void testLateBoundVolatileInstanceWithSingletonZ() throws BindException, InjectionException {
        Tang tang = Tang.Factory.getTang();
        ConfigurationBuilder cb = tang.newConfigurationBuilder();
        cb.bindSingletonImplementation(LateBoundVolatile.B.class, LateBoundVolatile.B.class);
        Injector i = tang.newInjector(cb.build());
        i.bindVolatileInstance(LateBoundVolatile.C.class, new LateBoundVolatile.C());
        i.getInstance(LateBoundVolatile.B.class);
    }
}

class LateBoundVolatile {
    static class A {
    }

    static class B extends A {
        @Inject
        B(C c) {
        }
    }

    static class C {
    }
}
