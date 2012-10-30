/*
 * Copyright 2012 Microsoft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.tang.implementation;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.ConfigurationBuilder;
import com.microsoft.tang.Injector;
import com.microsoft.tang.impl.Tang;
import com.microsoft.tang.impl.TangConf;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author Markus Weimer <mweimer@microsoft.com>
 */
public final class TangImpl implements com.microsoft.tang.Tang {

    @Override
    public final Injector getInjector(final Configuration... confs) {
        final TangConf[] tangConfigurations = new TangConf[confs.length];
        for (int i = 0; i < confs.length; ++i) {
            final Configuration c = confs[i];
            if (!(c instanceof ConfigurationImpl)) {
                throw new RuntimeException("Mixing TANG implementations no good, Yoda says!");
            }
            tangConfigurations[i] = ((ConfigurationImpl) c).getTangConf();
        }
        return new InjectorImpl(new Tang(tangConfigurations).forkConf().injector());
    }

    @Override
    public final Configuration configurationFromStream(final InputStream f) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public final ConfigurationBuilder newConfigurationBuilder() {
        return new ConfigurationBuilderImpl();
    }
}
