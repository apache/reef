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

import javax.inject.Inject;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class ConcurrentMapTable<K, V> implements MapTable<K, V> {
	
	
	private final ConcurrentMap<K, V> table = new ConcurrentHashMap<>();
	
	private Exception error = null;
	
	@Inject
	ConcurrentMapTable() {}

	@Override
	public void onNext(Entry<K, V> value) {
		table.put(value.getKey(), value.getValue());
	}

	@Override
	public void onError(Exception error) {
		this.error = error;
	}

	@Override
	public void onCompleted() {
		// TODO
	}

	@Override
	public V lookup(K key) throws Exception {
		if (this.error != null) throw this.error;
		return this.table.get(key);
	}

	@Override
	public boolean containsKey(K key) throws Exception {
		if (this.error != null) throw this.error;
		return this.table.containsKey(key);
	}


}
