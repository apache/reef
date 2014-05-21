/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.util;

/**
 * A basic Builder pattern interface.
 *
 * @param <T> The type of object to be built.
 */
public interface Builder<T> {

    /**
     * Builds a fresh instance of the object. This can be invoked several times,
     * each of which return a new instance.
     *
     * @return a fresh instance of the object.
     */
    public T build();
}
