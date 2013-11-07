/**
 * Copyright (C) 2013 Microsoft Corporation
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

import com.microsoft.wake.rx.Observer;

import java.util.Map.Entry;

public interface MapTable<K, V> extends Observer<Entry<K, V>> {

    /**
     * Lookup the value V assigned to key K
     *
     * @param key assigned to value V
     * @return the value V or null if not exists
     */
    public V lookup(K key) throws Exception;

    /**
     * Test for existence of key K
     *
     * @param key to test for existence
     * @return true if key K exists, false otherwise
     */
    public boolean containsKey(K key) throws Exception;
}
