/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.vortex.examples.matmul;

import org.apache.reef.io.serialization.Codec;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Encodes/decodes {@link MatMulOutput} to/from byte array.
 */
final class MatMulOutputCodec implements Codec<MatMulOutput> {

  @Override
  public byte[] encode(final MatMulOutput matMulOutput) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      try (DataOutputStream daos = new DataOutputStream(baos)) {
        final int index = matMulOutput.getIndex();
        final Matrix<Double> result = matMulOutput.getResult();

        daos.writeInt(index);
        encodeMatrixToStream(daos, result);

        return baos.toByteArray();
      }
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public MatMulOutput decode(final byte[] buf) {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(buf)) {
      try (DataInputStream dais = new DataInputStream(bais)) {
        final int index = dais.readInt();
        final Matrix result = decodeMatrixFromStream(dais);
        return new MatMulOutput(index, result);
      }
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Encode a Matrix to output stream.
   */
  private void encodeMatrixToStream(final DataOutputStream stream, final Matrix<Double> matrix) throws IOException {
    final int numRow = matrix.getNumRows();
    final int numColumn = matrix.getNumColumns();

    stream.writeInt(numRow);
    stream.writeInt(numColumn);

    for (final List<Double> row : matrix.getRows()) {
      for (final double element : row) {
        stream.writeDouble(element);
      }
    }
  }

  /**
   * Decode a Matrix from input stream.
   */
  private Matrix decodeMatrixFromStream(final DataInputStream stream) throws IOException {
    final int numRow = stream.readInt();
    final int numColumn = stream.readInt();

    final List<List<Double>> rows = new ArrayList<>(numRow);
    for (int rowIndex = 0; rowIndex < numRow; rowIndex++) {
      final List<Double> row = new ArrayList<>(numColumn);
      for (int columnIndex = 0; columnIndex < numColumn; columnIndex++) {
        row.add(stream.readDouble());
      }
      rows.add(row);
    }
    return new RowMatrix(Collections.unmodifiableList(rows));
  }
}
