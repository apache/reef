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
package org.apache.reef.client;

import org.apache.reef.common.AbstractFailure;
import org.apache.reef.proto.ReefServiceProtos.RuntimeErrorProto;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.remote.exception.RemoteRuntimeException;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Error message that REEF Client gets when there is an error in REEF resourcemanager.
 */
public final class FailedRuntime extends AbstractFailure {

  /**
   * Standard java logger.
   */
  private static final Logger LOG = Logger.getLogger(AbstractFailure.class.getName());

  /**
   * Codec to decode serialized exception from a byte array. It is used in getThrowable().
   */
  private static final ObjectSerializableCodec<Exception> CODEC = new ObjectSerializableCodec<>();

  /**
   * Create a new Failure object out of protobuf data.
   *
   * @param error Error message as a protocol buffers object.
   */
  public FailedRuntime(final RuntimeErrorProto error) {
    super(error.getIdentifier(), error.getMessage(), Optional.<String>empty(), Optional.of(getThrowable(error)),
        Optional.<byte[]>empty());
  }

  /**
   * Retrieve Java exception from protobuf object, if possible. Otherwise, return null.
   * This is a utility method used in the FailedRuntime constructor.
   *
   * @param error protobuf error message structure.
   * @return Java exception or null if exception is missing or cannot be decoded.
   */
  private static Throwable getThrowable(final RuntimeErrorProto error) {
    final byte[] data = getData(error);
    if (data != null) {
      try {
        return CODEC.decode(data);
      } catch (final RemoteRuntimeException ex) {
        LOG.log(Level.FINE, "Could not decode exception {0}: {1}", new Object[]{error, ex});
      }
    }
    return null;
  }

  /**
   * Get binary data for the exception, if it exists. Otherwise, return null.
   * This is a utility method used in the FailedRuntime constructor and getThrowable() method.
   *
   * @param error protobuf error message structure.
   * @return byte array of the exception or null if exception is missing.
   */
  private static byte[] getData(final RuntimeErrorProto error) {
    return error.hasException() ? error.getException().toByteArray() : null;
  }
}
