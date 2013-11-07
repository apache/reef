package com.microsoft.wake.storage;

import com.microsoft.wake.Identifier;

public class ReadResponse {
  final byte[] buf;
  final int bytesRead;
  final Identifier reqId;
  
  public ReadResponse(final byte[] buf, final int bytesRead, final Identifier reqId) {
    this.buf = buf;
    this.bytesRead = bytesRead;
    this.reqId = reqId;
  }
}