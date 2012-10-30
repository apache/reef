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
package com.microsoft.tang;

import com.microsoft.tang.implementation.TangImpl;
import java.io.IOException;
import java.io.InputStream;

/**
 * Main interface into TANG.
 * 
 * @author Markus Weimer <mweimer@microsoft.com>
 */
public interface Tang {

    /**
     * Returns an Injector for the given Configurations.
     * 
     * @param confs
     * @return 
     */
    public Injector getInjector(final Configuration... confs);

    /**
     * Reads a configuration from an InputStream
     * @param istream
     * @return
     * @throws IOException 
     */
    public Configuration configurationFromStream(final InputStream istream) throws IOException;

    /**
     * Create a new ConfigurationBuilder
     * 
     * @return a new ConfigurationBuilder
     */
    public ConfigurationBuilder newConfigurationBuilder();
    
    /**
     * Access to a Tang implementation
     */
    public final class Factory{
        public static Tang getTang(){
            return new TangImpl();
        }
    }
    
}
