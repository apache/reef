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
package org.apache.reef.driver.context;

import org.apache.reef.annotations.Provided;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Public;
import org.apache.reef.io.Message;
import org.apache.reef.io.Numberable;
import org.apache.reef.io.naming.Identifiable;

/**
 * Driver-side representation of a message sent by a Context to the Driver.
 */
@Public
@DriverSide
@Provided
public interface ContextMessage extends Message, Identifiable, Numberable {

  /**
   * @return the message sent by the Context.
   */
  @Override
  byte[] get();

  /**
   * @return the ID of the sending Context.
   */
  @Override
  String getId();

  /**
   * @return the ID of the ContextMessageSource that sent the message on the Context.
   */
  String getMessageSourceID();

  /**
   * @return the sequence number of the message
   *
   * Terminology: a source sends a message to a target.
   * Example sources are Tasks or Contexts. Example targets are Drivers.
   *
   * The method guarantees that sequence numbers increase strictly monotonically per message source.
   * So numbers of messages from different sources should not be compared to each other.
   *
   * Clients of this method must not assume any particular method implementation
   * because it may change in the future.
   *
   * The current implementation returns the timestamp of a heartbeat that contained the message.
   * A heartbeat may contain several messages; all such message will get the same sequence number.
   * A source can attach only one message to a single heartbeat, so the strict sequence number monotonicity
   * per source is guaranteed.
   *
   */
  @Override
  long getSequenceNumber();

}
