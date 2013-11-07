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

import com.microsoft.wake.EventHandler;
import com.microsoft.wake.rx.Observer;

import java.util.Collection;

/**
 * Non-blocking queue interface.
 * @param <M> the message type
 */
public interface Queue<M> extends EventHandler<M> {

	/**
	 * Drain all outstanding messages to the collection
	 * @param collection to drain to
	 * @throws Exception that was passed to {@link Observer#onError(Exception)}
	 */
	public void drainTo(Collection<M> collection);
	
	/**
	 * Add all elements to the queue
	 * @param collection
	 */
	public void addAll(Collection<M> collection);
	
	/**
	 * Non-blocking pull method
	 * @return next message or null if none exist
	 * @throws Exception that was passed to {@link Observer#onError(Exception)}
	 */
	public M pull();
	
}