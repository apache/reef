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

import com.microsoft.tang.Injector;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.exceptions.NameResolutionException;
import com.microsoft.tang.impl.TangInjector;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Markus Weimer <mweimer@microsoft.com>
 */
final class InjectorImpl implements Injector {

    private final TangInjector ti;

    InjectorImpl(TangInjector ti) {
        this.ti = ti;
    }

    @Override
    public <T> void bindVolatileInstance(final Class<T> clazz, final T object) {
        this.ti.bindVolatileInstance(clazz, object);
    }

    @Override
    public <U> U getInstance(final Class<U> clazz) throws InjectionException {
        try {
            return this.ti.getInstance(clazz);
        } catch (NameResolutionException | ReflectiveOperationException ex) {
            throw new InjectionException(ex);
        }
    }

    @Override
    public <T> T getNamedParameter(final Class<? extends Name<T>> clazz) throws InjectionException {
        try {
            return this.ti.getNamedParameter(clazz);
        } catch (NameResolutionException | ReflectiveOperationException ex) {
            throw new InjectionException(ex);
        }
    }
}
