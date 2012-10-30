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
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.exceptions.NameResolutionException;

/**
 * 
 * @author Markus Weimer <mweimer@microsoft.com>
 */
// TODO: Add examples to the javadoc above
public interface Injector {

    /**
     * Binds the given object to the class for the lifetime of this Injector.
     *
     * @param <T>
     * @param clazz
     * @param object
     * @throws BindException
     */
    public <T> void bindVolatileInstance(final Class<T> clazz, T object) throws BindException;

    /**
     * Gets an Instance of the class configured as the implementation for the
     * given interface class.
     *
     * @param <U>
     * @param clazz
     * @return an Instance of the class configured as the implementation for the
     * given interface class.
     * @throws InjectionException
     */
    public <U> U getInstance(final Class<U> clazz) throws InjectionException;

    /**
     * Gets the value stored for the given named parameter.
     * 
     * @param <T>
     * @param clazz
     * @return
     * @throws InjectionException
     */
    public <T> T getNamedParameter(final Class<? extends Name<T>> clazz) throws InjectionException;
}
