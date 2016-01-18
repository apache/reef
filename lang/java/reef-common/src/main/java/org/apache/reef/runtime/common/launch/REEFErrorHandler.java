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
package org.apache.reef.runtime.common.launch;

import com.google.protobuf.ByteString;
import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.runtime.common.launch.parameters.ErrorHandlerRID;
import org.apache.reef.runtime.common.launch.parameters.LaunchID;
import org.apache.reef.runtime.common.utils.ExceptionCodec;
import org.apache.reef.runtime.common.utils.RemoteManager;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * The error handler REEF registers with Wake.
 */
public final class REEFErrorHandler implements EventHandler<Throwable>, AutoCloseable {

  private static final Logger LOG = Logger.getLogger(REEFErrorHandler.class.getName());

  // This class is used as the ErrorHandler in the RemoteManager. Hence, we need an InjectionFuture here.
  private final InjectionFuture<RemoteManager> remoteManager;
  private final String launchID;
  private final String errorHandlerRID;
  private final ExceptionCodec exceptionCodec;

  @Inject
  REEFErrorHandler(final InjectionFuture<RemoteManager> remoteManager,
                   @Parameter(ErrorHandlerRID.class) final String errorHandlerRID,
                   @Parameter(LaunchID.class) final String launchID,
                   final ExceptionCodec exceptionCodec) {
    this.errorHandlerRID = errorHandlerRID;
    this.remoteManager = remoteManager;
    this.launchID = launchID;
    this.exceptionCodec = exceptionCodec;
  }

  @Override
  @SuppressWarnings("checkstyle:illegalcatch")
  public void onNext(final Throwable e) {
    LOG.log(Level.SEVERE, "Uncaught exception.", e);
    if (!this.errorHandlerRID.equals(ErrorHandlerRID.NONE)) {
      final EventHandler<ReefServiceProtos.RuntimeErrorProto> runtimeErrorHandler = this.remoteManager.get()
          .getHandler(errorHandlerRID, ReefServiceProtos.RuntimeErrorProto.class);
      final ReefServiceProtos.RuntimeErrorProto message = ReefServiceProtos.RuntimeErrorProto.newBuilder()
          .setName("reef")
          .setIdentifier(launchID)
          .setMessage(e.getMessage())
          .setException(ByteString.copyFrom(this.exceptionCodec.toBytes(e)))
          .build();
      try {
        runtimeErrorHandler.onNext(message);
      } catch (final Throwable t) {
        LOG.log(Level.SEVERE, "Unable to send the error upstream", t);
      }
    } else {
      LOG.log(Level.SEVERE, "Caught an exception from Wake we cannot send upstream because there is no upstream");
    }
  }

  @SuppressWarnings("checkstyle:illegalcatch")
  public void close() {
    try {
      this.remoteManager.get().close();
    } catch (final Throwable ex) {
      LOG.log(Level.SEVERE, "Unable to close the remote manager", ex);
    }
  }

  @Override
  public String toString() {
    return "REEFErrorHandler{" +
        "remoteManager=" + remoteManager +
        ", launchID='" + launchID + '\'' +
        ", errorHandlerRID='" + errorHandlerRID + '\'' +
        '}';
  }
}
