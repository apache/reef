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
package com.microsoft.reef.io.network.nggroup.impl;

import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.network.Connection;
import com.microsoft.reef.io.network.group.operators.AbstractGroupCommOperator;
import com.microsoft.reef.io.network.impl.NetworkService;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.util.StringIdentifierFactory;
import com.microsoft.wake.Identifier;
import com.microsoft.wake.IdentifierFactory;

import java.util.logging.Logger;

/**
 *
 */
public class Sender extends AbstractGroupCommOperator {

  private static final Logger LOG = Logger.getLogger(Sender.class.getName());

  private final NetworkService<GroupCommMessage> netService;
  private final IdentifierFactory idFac = new StringIdentifierFactory();

  public Sender(final NetworkService<GroupCommMessage> netService) {
    this.netService = netService;
  }

  public void send(final GroupCommMessage msg) throws NetworkException {
    final String dest = msg.getDestid();
    send(msg, dest);
  }

  public void send(final GroupCommMessage msg, final String dest) throws NetworkException {
    final Identifier destId = idFac.getNewInstance(dest);
    final Connection<GroupCommMessage> link = netService.newConnection(destId);
    link.open();
    link.write(msg);
  }
}
