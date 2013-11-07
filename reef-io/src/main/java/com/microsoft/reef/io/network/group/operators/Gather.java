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
package com.microsoft.reef.io.network.group.operators;

import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.network.group.impl.operators.basic.GatherOp;
import com.microsoft.tang.annotations.DefaultImplementation;
import com.microsoft.wake.Identifier;

import java.util.List;

/**
 * MPI Gather Operator
 * 
 * This is an operator where the root is a receiver and there are multiple
 * senders.
 * 
 * The root or receiver gathers all the elements sent by the senders in a List.
 * 
 * @author shravan
 * 
 */
public interface Gather {
	/**
	 * Senders or non-roots
	 * 
	 * @param <T>
	 */
  @DefaultImplementation(GatherOp.Sender.class)
	public static interface Sender<T> {
		/**
		 * Send the element to the root/receiver
		 * 
		 * @param element
		 * @throws InterruptedException
		 * @throws NetworkException
		 */
		public void send(T element) throws InterruptedException,
				NetworkException;
	}

	/**
	 * Receiver or Root
	 * 
	 * @param <T>
	 */
	@DefaultImplementation(GatherOp.Receiver.class)
	public static interface Receiver<T> {
		/**
		 * Receive the elements sent by the senders in default order
		 * 
		 * @return elements sent by senders as a List in default order
		 * @throws InterruptedException
		 * @throws NetworkException
		 */
		public List<T> receive() throws InterruptedException, NetworkException;

		/**
		 * Receive the elements sent by the senders in specified order
		 * 
		 * @param order
		 * @return elements sent by senders as a List in specified order
		 * @throws InterruptedException
		 * @throws NetworkException
		 */
		public List<T> receive(List<? extends Identifier> order)
				throws InterruptedException, NetworkException;
	}

}