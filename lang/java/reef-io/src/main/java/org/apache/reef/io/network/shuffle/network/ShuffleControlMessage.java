/**
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
package org.apache.reef.io.network.shuffle.network;

/**
 *
 */
public final class ShuffleControlMessage extends ShuffleMessage<byte[]> {

  public static final byte MANAGER = 0;
  public static final byte CONTROLLER = 1;
  public static final byte CLIENT = 2;
  public static final byte SENDER = 3;
  public static final byte RECEIVER = 4;

  private int code;
  private final byte[][] data;

  private byte sourceType;
  private byte sinkType;

  public ShuffleControlMessage(
      final int code,
      final String shuffleName,
      final String groupingName,
      final byte[][] data,
      final byte sourceType,
      final byte sinkType) {
    super(shuffleName, groupingName);
    this.code = code;
    this.data = data;
    this.sourceType = sourceType;
    this.sinkType = sinkType;
  }

  public int getCode() {
    return code;
  }

  @Override
  public int size() {
    if (data == null) {
      return 0;
    }

    return data.length;
  }

  @Override
  public byte[] get(final int index) {
    return data[index];
  }

  public byte getSourceType() {
    return sourceType;
  }

  public byte getSinkType() {
    return sinkType;
  }

  public boolean isMessageFromManager() {
    return sourceType == MANAGER;
  }

  public boolean isMessageFromController() {
    return sourceType == CONTROLLER;
  }

  public boolean isMessageFromClient() {
    return sourceType == CLIENT;
  }

  public boolean isMessageFromSender() {
    return sourceType == SENDER;
  }

  public boolean isMessageFromReceiver() {
    return sourceType == RECEIVER;
  }

  public boolean isMessageToManager() {
    return sinkType == MANAGER;
  }

  public boolean isMessageToController() {
    return sinkType == CONTROLLER;
  }

  public boolean isMessageToClient() {
    return sinkType == CLIENT;
  }

  public boolean isMessageToSender() {
    return sinkType == SENDER;
  }

  public boolean isMessageToReceiver() {
    return sinkType == RECEIVER;
  }
}
