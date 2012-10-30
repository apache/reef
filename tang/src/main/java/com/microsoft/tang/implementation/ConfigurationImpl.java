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
import com.microsoft.tang.impl.TangConf;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

/**
 *
 * @author Markus Weimer <mweimer@microsoft.com>
 */
final class ConfigurationImpl implements Configuration {

    private final TangConf tc;

    ConfigurationImpl(final TangConf tc) {
        this.tc = tc;
    }
    
    @Override
    public void writeConfigurationToStream(final OutputStream s) throws IOException {        
        final PrintStream ps = new PrintStream(s);
        this.tc.writeConfigurationFile(ps);        
    }

    TangConf getTangConf() {
        return tc;
    }
}
