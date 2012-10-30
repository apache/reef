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
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.impl.Tang;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Markus Weimer <mweimer@microsoft.com>
 */
final class ConfigurationBuilderImpl implements ConfigurationBuilder {

    private final Tang t;

    ConfigurationBuilderImpl() {
        this.t = new Tang();
    }

    @Override
    public void addConfiguration(Configuration conf) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public <T> void bindImplementation(final Class<T> iface, final Class<? extends T> impl) throws BindException {
        t.bindImplementation(iface, impl);
    }

    @Override
    public <T> void bindSingletonImplementation(Class<T> iface, Class<? extends T> impl) throws BindException {
        try {
            t.bindSingleton(iface, impl);
        } catch (ReflectiveOperationException ex) {
            throw new BindException(ex);
        }
    }

    @Override
    public <T> void bindNamedParameter(Class<? extends Name<T>> name, String value) throws BindException {
        t.bindParameter(name, value);        
    }

    @Override
    public Configuration build() {
        return new ConfigurationImpl(t.forkConf());
    }
}
