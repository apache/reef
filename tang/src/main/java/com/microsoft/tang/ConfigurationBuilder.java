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

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.exceptions.BindException;

/**
 * A builder for TANG Configurations.
 *
 * @author Markus Weimer <mweimer@microsoft.com>
 */
public interface ConfigurationBuilder {

    /**
     * Add all configuration parameters from the given Configuration object.
     *
     * @param conf
     */
    public void addConfiguration(final Configuration conf) throws BindException;

    /**
     * Binds the Class impl as the implementation of the interface iface
     *
     * @param <T>
     * @param iface
     * @param impl
     * @throws BindException
     */
    public <T> void bindImplementation(final Class<T> iface, final Class<? extends T> impl) throws BindException;

    /**
     * Same as bindImplementation, but with Singleton semantics. Only one object
     * of impl will be created.
     *
     * @param <T>
     * @param iface
     * @param impl
     * @throws BindException
     */
    public <T> void bindSingletonImplementation(final Class<T> iface, final Class<? extends T> impl) throws BindException;

    /**
     * Binds the String value to the named parameter name.
     *
     * @param <T>
     * @param name
     * @param value
     * @throws BindException
     */
    public <T> void bindNamedParameter(final Class<? extends Name<T>> name, final String value) throws BindException;

    public <T> void bindConstructor(Class<T> iface, Class<? extends ExternalConstructor<? extends T>> constructor) throws BindException;

    /**
     * Builds the immutable Configuration object.
     *
     * @return a new immutable Configuration object
     */
    public Configuration build();
}
