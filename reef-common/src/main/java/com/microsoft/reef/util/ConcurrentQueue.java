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
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ConcurrentQueue<T> implements Queue<T> {
	
	private final BlockingQueue<T> queue = new LinkedBlockingQueue<>();
	
	@Inject
	public ConcurrentQueue() {}

	@Override
	public void onNext(T value) {
		this.queue.add(value);
	}

	@Override
	public void drainTo(Collection<T> collection) {
		this.queue.drainTo(collection);
	}

	@Override
	public T pull() {
		return this.queue.poll();
	}

	@Override
	public void addAll(Collection<T> collection) {
		this.queue.addAll(collection);
	}

}