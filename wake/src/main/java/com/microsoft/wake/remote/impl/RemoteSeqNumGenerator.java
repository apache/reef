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

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Generates the sequence number for remote messages per destination 
 */
public class RemoteSeqNumGenerator {
  
  private final ConcurrentMap<SocketAddress, AtomicLong> seqMap;

  public RemoteSeqNumGenerator() {
    seqMap = new ConcurrentHashMap<SocketAddress, AtomicLong> ();
  }
  
  public long getNextSeq(SocketAddress addr) {
    AtomicLong seq = seqMap.get(addr);
    if (seq == null) {
      seq = new AtomicLong(0);
      if (seqMap.putIfAbsent(addr, seq) != null) {
        seq = seqMap.get(addr);
      }
    }
    return seq.getAndIncrement();
  }
  
}
