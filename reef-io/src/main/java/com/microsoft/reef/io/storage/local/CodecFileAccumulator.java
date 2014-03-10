package com.microsoft.reef.io.storage.local;

import com.microsoft.reef.exception.evaluator.ServiceException;
import com.microsoft.reef.exception.evaluator.StorageException;
import com.microsoft.reef.io.Accumulator;
import com.microsoft.reef.io.serialization.Codec;

import java.io.*;

final class CodecFileAccumulator<T> implements Accumulator<T> {

  private final Codec<T> codec;
  private final ObjectOutputStream out;

  public CodecFileAccumulator(Codec<T> codec, final File file) throws IOException {
    this.codec = codec;
    this.out = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
  }

  @Override
  public void add(T datum) throws ServiceException {
    byte[] buf = codec.encode(datum);
    try {
      out.writeInt(buf.length);
      out.write(buf);
    } catch (IOException e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void close() throws ServiceException {
    try {
      this.out.writeInt(-1);
      this.out.close();
    } catch (final IOException e) {
      throw new ServiceException(e);
    }
  }
}
