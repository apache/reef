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

import com.microsoft.reef.task.Task;
import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.group.operators.Scatter;

import javax.inject.Inject;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ComputeTask Receives the partial matrix(row partitioned) it is
 * responsible for. Also receives one column vector per iteration and computes
 * the partial product of this vector with its assigned partial matrix The
 * partial product across all the compute tasks are concatenated by doing a
 * Reduce with Concat as the Reduce Function
 * 
 * @author shravan
 * 
 */
public class ComputeTask implements Task {
  private final Logger logger = Logger.getLogger(ComputeTask.class
      .getName());
	/**
	 * The Group Communication Operators that are needed by this task. These
	 * will be injected into the constructor by TANG. The operators used here
	 * are complementary to the ones used in the ControllerTask
	 */
	Scatter.Receiver<Vector> scatterReceiver;
	Broadcast.Receiver<Vector> broadcastReceiver;
	Reduce.Sender<Vector> reduceSender;

	/**
	 * This class is instantiated by TANG
	 * 
	 * @param scatterReceiver
	 *            The receiver for the scatter operation
	 * @param broadcastReceiver
	 *            The receiver for the broadcast operation
	 * @param reduceSender
	 *            The sender for the reduce operation
	 */
	@Inject
	public ComputeTask(Scatter.Receiver<Vector> scatterReceiver,
                     Broadcast.Receiver<Vector> broadcastReceiver,
                     Reduce.Sender<Vector> reduceSender) {
		super();
		this.scatterReceiver = scatterReceiver;
		this.broadcastReceiver = broadcastReceiver;
		this.reduceSender = reduceSender;
	}

	@Override
	public byte[] call(byte[] memento) throws Exception {
		// Receive the partial matrix using which
		// we compute the dot products
	  logger.log(Level.FINE, "Waiting for scatterReceive");
		List<Vector> partialA = scatterReceiver.receive();
		logger.log(Level.FINE, "Received: " + partialA);
		// Receive how many times we need to do the
		// dot product
		Vector sizeVec = broadcastReceiver.receive();
		int size = (int) sizeVec.get(0);
		for (int i = 0; i < size; i++) {
			// Receive column vector
			Vector x = broadcastReceiver.receive();
			// Compute partial product Ax
			Vector partialAx = computeAx(partialA, x);
			// Send up the aggregation(concatenation) tree
			// to the controller task
			reduceSender.send(partialAx);
		}
		return null;
	}

	private Vector computeAx(List<Vector> partialA, Vector x) {
		Vector result = new DenseVector(partialA.size());
		int i = 0;
		for (Vector row : partialA) {
			result.set(i++, row.dot(x));
		}
		return result;
	}
}
