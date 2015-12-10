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
package org.apache.reef.runtime.mesos.util;

import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.exception.RemoteRuntimeException;

import javax.inject.Inject;
import java.io.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * EvaluatorControl codec.
 */
public class MesosRemoteManagerCodec implements Codec<EvaluatorControl> {
  private static final Logger LOG = Logger.getLogger(MesosRemoteManagerCodec.class.getName());

  @Inject
  public MesosRemoteManagerCodec() {
  }

  @Override
  public byte[] encode(final EvaluatorControl evaluatorControl) {
    try {
      LOG.log(Level.FINEST, "Before Encoding: {0}", evaluatorControl.toString());
      final ByteArrayOutputStream out = new ByteArrayOutputStream();
      final BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
      final DatumWriter<EvaluatorControl> writer = new SpecificDatumWriter<>(EvaluatorControl.getClassSchema());
      writer.write(evaluatorControl, encoder);
      encoder.flush();
      out.close();
      LOG.log(Level.FINEST, "After Encoding");
      return out.toByteArray();
    } catch (final IOException ex) {
      throw new RemoteRuntimeException(ex);
    }
  }

  @Override
  public EvaluatorControl decode(final byte[] buf) {
    try {
      LOG.log(Level.FINEST, "Before Decoding: {0}", buf);
      final SpecificDatumReader<EvaluatorControl> reader = new SpecificDatumReader<>(EvaluatorControl.getClassSchema());
      final Decoder decoder = DecoderFactory.get().binaryDecoder(buf, null);
      LOG.log(Level.FINEST, "After Decoding");
      return reader.read(null, decoder);
    } catch (final IOException ex) {
      throw new RemoteRuntimeException(ex);
    }
  }
}
