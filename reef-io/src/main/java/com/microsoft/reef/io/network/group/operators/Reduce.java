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
import com.microsoft.reef.io.network.group.impl.operators.basic.ReduceOp;
import com.microsoft.tang.annotations.DefaultImplementation;
import com.microsoft.wake.Identifier;

import java.util.List;

/**
 * MPI Reduce operator
 * 
 * This is another operator with root being receiver All senders send an element
 * to the receiver. These elements are passed through a reduce function and its
 * result is made available at the root
 * 
 * @author shravan
 * 
 */
public interface Reduce {
	/**
	 * Receiver or Root
	 * 
	 * @param <T>
	 */
  @DefaultImplementation(ReduceOp.Receiver.class)
	public static interface Receiver<T> {
		/**
		 * Receive values sent by senders and pass them through the reduce
		 * function in default order
		 * 
		 * @return Result of applying reduce function on the elements gathered
		 *         in default order
		 * @throws InterruptedException
		 * @throws NetworkException
		 */
		public T reduce() throws InterruptedException, NetworkException;

		/**
		 * Receive values sent by senders and pass them through the reduce
		 * function in specified order
		 * 
		 * @param order
		 * @return Result of applying reduce function on the elements gathered
		 *         in specified order
		 * @throws InterruptedException
		 * @throws NetworkException
		 */
		public T reduce(List<? extends Identifier> order)
				throws InterruptedException, NetworkException;

		/**
		 * The reduce function to be applied on the set of received values
		 * 
		 * @return {@link ReduceFunction}
		 */
		public Reduce.ReduceFunction<T> getReduceFunction();
	}

	/**
	 * Senders or non roots
	 * 
	 * @param <T>
	 */
  @DefaultImplementation(ReduceOp.Sender.class)
	public static interface Sender<T> {
		/**
		 * Send the element to the root
		 * 
		 * @param element
		 * @throws NetworkException
		 * @throws InterruptedException
		 */
		public void send(T element) throws NetworkException,
				InterruptedException;

		/**
		 * The {@link ReduceFunction} to be applied on the set of received values
		 * 
		 * @return {@link ReduceFunction}
		 */
		public Reduce.ReduceFunction<T> getReduceFunction();
	}

	/**
	 * Interface for a Reduce Function takes in an {@link Iterable} returns an
	 * aggregate value computed from the {@link Iterable}
	 * 
	 * @param <T>
	 */
	public static interface ReduceFunction<T> {
		/**
		 * Apply the function on elements
		 * 
		 * @param elements
		 * @return aggregate value computed from elements
		 */
		public T apply(Iterable<T> elements);
	}
}