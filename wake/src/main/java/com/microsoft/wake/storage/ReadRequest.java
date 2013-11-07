package com.microsoft.wake.storage;

import com.microsoft.wake.Identifiable;
import com.microsoft.wake.Identifier;

public class ReadRequest implements Identifiable{
  final StorageIdentifier f;
  final long offset;
  final byte[] buf;
  final Identifier id;
  public ReadRequest(StorageIdentifier f, long offset, byte[] buf, Identifier id) {
    this.f = f;
    this.offset = offset;
    this.buf = buf;
    this.id = id;
  }
  @Override
  public Identifier getId() {
    return id;
  }
}
