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
package org.apache.reef.io.network.group.impl.driver;


import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.impl.NetworkService;
import org.apache.reef.io.network.group.impl.GroupCommunicationMessage;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;

import java.util.logging.Logger;

/**
 * Event handler that receives ctrl msgs and.
 * dispatched them using network service
 */
public class CtrlMsgSender implements EventHandler<GroupCommunicationMessage> {

  private static final Logger LOG = Logger.getLogger(CtrlMsgSender.class.getName());
  private final IdentifierFactory idFac;
  private final NetworkService<GroupCommunicationMessage> netService;

  public CtrlMsgSender(final IdentifierFactory idFac, final NetworkService<GroupCommunicationMessage> netService) {
    this.idFac = idFac;
    this.netService = netService;
  }

  @Override
  public void onNext(final GroupCommunicationMessage srcCtrlMsg) {
    LOG.entering("CtrlMsgSender", "onNext", srcCtrlMsg);
    final Identifier id = idFac.getNewInstance(srcCtrlMsg.getDestid());
    final Connection<GroupCommunicationMessage> link = netService.newConnection(id);
    try {
      link.open();
      link.write(srcCtrlMsg);
    } catch (final NetworkException e) {
      throw new RuntimeException("Unable to send ctrl task msg to parent " + id, e);
    }
    LOG.exiting("CtrlMsgSender", "onNext", srcCtrlMsg);
  }

}
