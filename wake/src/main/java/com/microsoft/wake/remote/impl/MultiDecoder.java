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
package com.microsoft.wake.remote.impl;

import java.util.Map;

import com.microsoft.wake.remote.proto.WakeRemoteProtos.WakeTuplePBuf;

import com.google.protobuf.InvalidProtocolBufferException;
import com.microsoft.wake.remote.Decoder;
import com.microsoft.wake.remote.exception.RemoteRuntimeException;

/**
 * Decoder using the WakeTuple protocol buffer
 * (class name and bytes)
 *
 * @param <T> type
 */
public class MultiDecoder<T> implements Decoder<T> {
  private final Map<Class<? extends T>, Decoder<? extends T>> clazzToDecoderMap;

  /**
   * Constructs a decoder that decodes bytes based on the class name
   * 
   * @param clazzToDecoderMap
   */
  public MultiDecoder(Map<Class<? extends T>, Decoder<? extends T>> clazzToDecoderMap) {
    this.clazzToDecoderMap = clazzToDecoderMap;
  }
  
  /**
   * Decodes byte array 
   * 
   * @param data class name and byte payload
   */
  @Override
  public T decode(byte[] data) {
    WakeTuplePBuf tuple;
    try {
      tuple = WakeTuplePBuf.parseFrom(data);
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
      throw new RemoteRuntimeException(e);
    }
    
    String className = tuple.getClassName();
    byte[] message = tuple.getData().toByteArray();
    Class<?> clazz;
    try {
      clazz = Class.forName(className);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      throw new RemoteRuntimeException(e);
    }
    return clazzToDecoderMap.get(clazz).decode(message);
  }  
}
