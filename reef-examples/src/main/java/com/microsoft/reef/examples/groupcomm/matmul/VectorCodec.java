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

import com.microsoft.wake.remote.Codec;

import javax.inject.Inject;
import java.io.*;

/**
 * Codec for the Vector type Uses Data*Stream
 * 
 * @author shravan
 * 
 */
public class VectorCodec implements Codec<Vector> {
	/**
	 * This class is instantiated by TANG
	 */
	@Inject
	public VectorCodec() {
		// Intentionally blank
	}

	@Override
	public Vector decode(byte[] data) {
		ByteArrayInputStream bais = new ByteArrayInputStream(data);
		Vector result;
		try (DataInputStream dais = new DataInputStream(bais)) {
			int size = dais.readInt();
			result = new DenseVector(size);
			for (int i = 0; i < size; i++)
				result.set(i, dais.readDouble());
		} catch (IOException e) {
			throw new RuntimeException(e.getCause());
		}
		return result;
	}

	@Override
	public byte[] encode(Vector vec) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(vec.size()
				* Double.SIZE);
		try (DataOutputStream daos = new DataOutputStream(baos)) {
			daos.writeInt(vec.size());
			for (int i = 0; i < vec.size(); i++) {
				daos.writeDouble(vec.get(i));
			}
		} catch (IOException e) {
			throw new RuntimeException(e.getCause());
		}
		return baos.toByteArray();
	}

}
