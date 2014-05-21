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
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ControllerTask Controls the matrix multiplication task Splits up the
 * input matrix into parts(row-wise) and scatters them amongst the compute
 * tasks Broadcasts each column vector Receives the reduced(concatenated)
 * partial products as the output vector
 * 
 * @author shravan
 * 
 */
public class ControllerTask implements Task {

  private final Logger logger = Logger.getLogger(ControllerTask.class.getName());

	/**
	 * The Group Communication Operators that are needed by this task. These
	 * will be injected into the constructor by TANG. The operators used here
	 * are complementary to the ones used in the ComputeTask
	 */
	Scatter.Sender<Vector> scatterSender;
	Broadcast.Sender<Vector> broadcastSender;
	Reduce.Receiver<Vector> reduceReceiver;

	// The matrices
	List<Vector> X, A;

	// We compute AX'

	/**
	 * This class is instantiated by TANG
	 * 
	 * @param scatterSender
	 *            The sender for the scatter operation
	 * @param broadcastSender
	 *            The sender for the broadcast operation
	 * @param reduceReceiver
	 *            The receiver for the reduce operation
	 */
	@Inject
	public ControllerTask(Scatter.Sender<Vector> scatterSender,
                        Broadcast.Sender<Vector> broadcastSender,
                        Reduce.Receiver<Vector> reduceReceiver) {
		super();
		this.scatterSender = scatterSender;
		this.broadcastSender = broadcastSender;
		this.reduceReceiver = reduceReceiver;

		// For now initialize the matrix
		// TODO: Read from disk/hdfs
		int[][] matrix = { 
				{ 0, 1, 2, 3, 4 }, 
				{ 5, 6, 7, 8, 9 },
				{ 10, 11, 12, 13, 14 }, 
				{ 15, 16, 17, 18, 19 },
				{ 20, 21, 22, 23, 24 } };

		// Convert matrix into a list of row vectors
		A = new ArrayList<>(matrix.length);
		for (int i = 0; i < matrix.length; i++) {
			Vector rowi = new DenseVector(5);
			for (int j = 0; j < matrix[i].length; j++) {
				rowi.set(j, matrix[i][j]);
			}
			A.add(rowi);
		}

		// Setting X = A for now. Hence computing AA'
		// TODO: Read X from disk/hdfs
		X = A;
	}

	/**
	 * Computes AX'
	 */
	@Override
	public byte[] call(byte[] memento) throws Exception {
		// Scatter the matrix A
	  logger.log(Level.FINE, "Scattering A");
		scatterSender.send(A);
		logger.log(Level.FINE, "Finished Scattering A");
		List<Vector> result = new ArrayList<>();
		Vector sizeVec = new DenseVector(1);
		sizeVec.set(0, (double) X.size());
		// Broadcast the number of columns to be
		// broadcasted
		broadcastSender.send(sizeVec);
		// Just use Iterable with a Matrix class
		for (Vector x : X) {
			// Broadcast each column
			broadcastSender.send(x);
			// Receive a concatenated vector of the
			// partial sums computed by each computeTask
			Vector Ax = reduceReceiver.reduce();
			// Accumulate the result
			result.add(Ax);
		}

		String resStr = resultString(A, X, result);
		return resStr.getBytes();
	}

	/**
	 * Construct the display string and send it to the driver
	 * 
	 * @param A
	 * @param X
	 * @param result
	 *            = AX'
	 * @return A string indicating the matrices being multiplied and the result
	 */
	private String resultString(List<Vector> A, List<Vector> X,
			List<Vector> result) {
		StringBuilder sb = new StringBuilder();

		int[][] a = new int[A.size()][A.get(0).size()];
		for (int i = 0; i < a.length; i++) {
			Vector rowi = A.get(i);
			for (int j = 0; j < rowi.size(); j++)
				a[i][j] = (int) rowi.get(j);
		}

		int[][] x = new int[X.size()][X.get(0).size()];
		for (int j = 0; j < X.size(); j++) {
			Vector colj = X.get(j);
			for (int i = 0; i < colj.size(); i++) {
				x[i][j] = (int) colj.get(i);
			}
		}

		int[][] res = new int[result.size()][result.get(0).size()];
		for (int j = 0; j < result.size(); j++) {
			Vector colj = result.get(j);
			for (int i = 0; i < colj.size(); i++)
				res[i][j] = (int) colj.get(i);
		}

		for (int i = 0; i < a.length; i++) {
			if (i != (a.length / 2))
				sb.append(rowString(a[i]) + "       " + rowString(x[i])
						+ "         " + rowString(res[i]) + "\n");
			else
				sb.append(rowString(a[i]) + "    X  " + rowString(x[i])
						+ "    =    " + rowString(res[i]) + "\n");
		}

		return sb.toString();
	}

	private String rowString(int[] a) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < a.length; i++) {
			sb.append(String.format("%4d", a[i]));
			if (i < a.length - 1)
				sb.append(", ");
		}
		return sb.toString();
	}
}
