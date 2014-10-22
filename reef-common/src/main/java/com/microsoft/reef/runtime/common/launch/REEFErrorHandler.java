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
package com.microsoft.reef.runtime.common.launch;

import com.google.protobuf.ByteString;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.runtime.common.launch.parameters.ErrorHandlerRID;
import com.microsoft.reef.runtime.common.launch.parameters.LaunchID;
import com.microsoft.reef.runtime.common.utils.ExceptionCodec;
import com.microsoft.reef.runtime.common.utils.RemoteManager;
import com.microsoft.tang.InjectionFuture;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * The error handler REEF registers with Wake.
 */
public final class REEFErrorHandler implements EventHandler<Throwable> {

  private static final Logger LOG = Logger.getLogger(REEFErrorHandler.class.getName());

  // This class is used as the ErrorHandler in the RemoteManager. Hence, we need an InjectionFuture here.
  private final InjectionFuture<RemoteManager> remoteManager;
  private final String launchID;
  private final String errorHandlerRID;
  private final ExceptionCodec exceptionCodec;

  @Inject
  REEFErrorHandler(final InjectionFuture<RemoteManager> remoteManager,
                   final @Parameter(ErrorHandlerRID.class) String errorHandlerRID,
                   final @Parameter(LaunchID.class) String launchID,
                   final ExceptionCodec exceptionCodec) {
    this.errorHandlerRID = errorHandlerRID;
    this.remoteManager = remoteManager;
    this.launchID = launchID;
    this.exceptionCodec = exceptionCodec;
  }

  @Override
  public void onNext(final Throwable e) {
    LOG.log(Level.SEVERE, "Uncaught exception.", e);
    // TODO: This gets a new EventHandler each time an exception is caught. It would be better to cache the handler. But
    // that introduces threading issues and isn't really worth it, as the JVM typically will be killed once we catch an
    // Exception in here.
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
