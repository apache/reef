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
package com.microsoft.reef.io.network.nggroup.api;

import com.microsoft.reef.io.network.group.operators.Reduce.ReduceFunction;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.reef.io.serialization.Codec;

import java.util.List;

/**
 *
 */
public interface OperatorTopology {

  /**
   * @param msg
   */
  void handle(GroupCommMessage msg);


  /**
   * @param data
   * @param msgType
   */
  void sendToChildren(byte[] data, Type msgType);


  /**
   * @return
   */
  byte[] recvFromParent();


  /**
   * @return
   */
  List<byte[]> recvFromChildren();


  /**
   * @return
   */
  String getSelfId();


  /**
   * @param encode
   * @param reduce
   */
  void sendToParent(byte[] encode, Type reduce);


  /**
   *
   */
  void initialize();


  /**
   * @param redFunc
   * @param dataCodec
   * @return
   */
  <T> T recvFromChildren(ReduceFunction<T> redFunc, Codec<T> dataCodec);

}
