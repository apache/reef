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
package com.microsoft.reef.examples.groupcomm.matmul;

import com.microsoft.reef.io.network.group.operators.Reduce;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

/**
 * A Reduce function that concatenates an iterable of vectors into a single
 * vector
 * 
 * @author shravan
 * 
 */
public class VectorConcat implements Reduce.ReduceFunction<Vector> {

	@Inject
	public VectorConcat() {
	}

	@Override
	public Vector apply(Iterable<Vector> elements) {
		List<Double> resultLst = new ArrayList<>();
		for (Vector element : elements) {
			for (int i = 0; i < element.size(); i++)
				resultLst.add(element.get(i));
		}
		Vector result = new DenseVector(resultLst.size());
		int i = 0;
		for (double elem : resultLst)
			result.set(i++, elem);
		return result;
	}

}