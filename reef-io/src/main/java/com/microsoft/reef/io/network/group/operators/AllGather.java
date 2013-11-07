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
import com.microsoft.reef.io.network.group.impl.operators.basic.AllGatherOp;
import com.microsoft.tang.annotations.DefaultImplementation;
import com.microsoft.wake.Identifier;

import java.util.List;

/**
 * MPI AllGather Operator
 * 
 * Each activity applies this operator on an element of type T. The result will be
 * a list of elements constructed using the elements all-gathered at each
 * activity.
 * 
 * @author shravan
 * 
 * @param <T>
 */
@DefaultImplementation(AllGatherOp.class)
public interface AllGather<T> {

	/**
	 * Apply the operation on element
	 * 
	 * @param element
	 * @return List of all elements on which the operation was applied using
	 *         default order
	 * @throws NetworkException
	 * @throws InterruptedException
	 */
	public List<T> apply(T element) throws NetworkException,
			InterruptedException;

	/**
	 * Apply the operation on element
	 * 
	 * @param element
	 * @param order
	 * @return List of all elements on which the operation was applied using
	 *         order specified
	 * @throws NetworkException
	 * @throws InterruptedException
	 */
	public List<T> apply(T element, List<? extends Identifier> order)
			throws NetworkException, InterruptedException;
}