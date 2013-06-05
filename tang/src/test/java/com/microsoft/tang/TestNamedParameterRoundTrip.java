package com.microsoft.tang;

import static org.junit.Assert.*;

import org.junit.Test;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.ConfigurationFile;

public class TestNamedParameterRoundTrip {

    @NamedParameter()
    public final class Dimensionality implements Name<Integer> {
        // Intentionally Empty
    }

    /**
     * Break criterion for the optimizer. If the progress in mean loss between
     * two iterations is less than this, the optimization stops.
     */
    @NamedParameter()
    public final class Eps implements Name<Double> {
        // Intentionally Empty
    }

    @Test
    public void testRoundTrip() throws BindException, InjectionException {
        final int d = 10;
        final double eps = 1e-5;
        final JavaConfigurationBuilder b = Tang.Factory.getTang().newConfigurationBuilder();
        b.bindNamedParameter(Dimensionality.class, String.valueOf(d));
        b.bindNamedParameter(Eps.class, String.valueOf(eps));
        final Configuration conf = b.build();

        {
            final Injector i = Tang.Factory.getTang().newInjector(conf);

            final int readD = i.getNamedInstance(Dimensionality.class).intValue();
            final double readEps = i.getNamedInstance(Eps.class).doubleValue();

            assertEquals(eps, readEps, 1e-12);
            assertEquals(d, readD);
        }
                
        
        {
            JavaConfigurationBuilder roundTrip = Tang.Factory.getTang().newConfigurationBuilder();
            ConfigurationFile.addConfiguration(roundTrip, ConfigurationFile.toConfigurationString(conf));
            final Injector i = Tang.Factory.getTang().newInjector(roundTrip.build());

            final int readD = i.getNamedInstance(Dimensionality.class).intValue();
            final double readEps = i.getNamedInstance(Eps.class).doubleValue();

            assertEquals(eps, readEps, 1e-12);
            assertEquals(d, readD);
        }
        
        {
            final Injector parent = Tang.Factory.getTang().newInjector(Tang.Factory.getTang().newConfigurationBuilder().build());
            final Injector i = parent.createChildInjector(conf);

            final int readD = i.getNamedInstance(Dimensionality.class).intValue();
            final double readEps = i.getNamedInstance(Eps.class).doubleValue();

            assertEquals(eps, readEps, 1e-12);
            assertEquals(d, readD);
        }
        
        {
            final Injector parent = Tang.Factory.getTang().newInjector(Tang.Factory.getTang().newConfigurationBuilder().build());
            final JavaConfigurationBuilder roundTrip = Tang.Factory.getTang().newConfigurationBuilder();
            ConfigurationFile.addConfiguration(roundTrip,
                ConfigurationFile.toConfigurationString(conf));
            final Injector i = parent.createChildInjector(roundTrip.build());

            final int readD = i.getNamedInstance(Dimensionality.class).intValue();
            final double readEps = i.getNamedInstance(Eps.class).doubleValue();

            assertEquals(eps, readEps, 1e-12);
            assertEquals(d, readD);
        }

    }

}
