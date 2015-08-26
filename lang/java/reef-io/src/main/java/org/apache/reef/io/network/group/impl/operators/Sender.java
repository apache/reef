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
package org.apache.reef.io.network.group.impl.operators;

import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.group.api.operators.AbstractGroupCommOperator;
import org.apache.reef.io.network.impl.NetworkService;
import org.apache.reef.io.network.group.impl.GroupCommunicationMessage;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;

import java.util.logging.Logger;

public class Sender extends AbstractGroupCommOperator {

  private static final Logger LOG = Logger.getLogger(Sender.class.getName());

  private final NetworkService<GroupCommunicationMessage> netService;
  private final IdentifierFactory idFac = new StringIdentifierFactory();

  public Sender(final NetworkService<GroupCommunicationMessage> netService) {
    this.netService = netService;
  }

  public void send(final GroupCommunicationMessage msg) throws NetworkException {
    LOG.entering("Sender", "send", msg);
    final String dest = msg.getDestid();
    send(msg, dest);
    LOG.exiting("Sender", "send", msg);
  }

  public void send(final GroupCommunicationMessage msg, final String dest) throws NetworkException {
    LOG.entering("Sender", "send", msg);
    final Identifier destId = idFac.getNewInstance(dest);
    final Connection<GroupCommunicationMessage> link = netService.newConnection(destId);
    link.open();
    link.write(msg);
    LOG.exiting("Sender", "send", msg);
  }
}
