package com.microsoft.reef.io.storage.local;

import com.microsoft.reef.exception.evaluator.ServiceRuntimeException;
import com.microsoft.reef.exception.evaluator.StorageException;
import com.microsoft.reef.io.serialization.Codec;

import java.io.*;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

final class CodecFileIterator<T> implements Iterator<T> {
  private static final Logger LOG = Logger.getLogger(CodecFileIterator.class.getName());
  private final Codec<T> codec;
  private int sz;

  private final ObjectInputStream in;

  CodecFileIterator(final Codec<T> codec, final File file) throws IOException {
    this.in = new ObjectInputStream(new BufferedInputStream(new FileInputStream(file)));
    this.codec = codec;
    this.sz = in.readInt();
  }

  @Override
  public boolean hasNext() {
    final boolean result = (sz != -1);
    if (!result) {
      try {
        this.in.close();
      } catch (final IOException e) {
        LOG.log(Level.WARNING, "Unable to close input stream.", e);
      }
    }
    return result;
  }

  @Override
  public T next() {
    try {
      final byte[] buf = new byte[sz];
      for (int rem = buf.length; rem > 0; rem -= in.read(buf, buf.length
          - rem, rem)) {
      }
      this.sz = in.readInt();
      if (this.sz == -1) {
        in.close();
      }
      return codec.decode(buf);
    } catch (IOException e) {
      throw new ServiceRuntimeException(new StorageException(e));
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Attempt to remove value from read-only input file!");
  }
}
