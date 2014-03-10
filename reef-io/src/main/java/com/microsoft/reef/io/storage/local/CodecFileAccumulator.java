package com.microsoft.reef.io.storage.local;

import com.microsoft.reef.exception.evaluator.ServiceException;
import com.microsoft.reef.exception.evaluator.StorageException;
import com.microsoft.reef.io.Accumulator;
import com.microsoft.reef.io.serialization.Codec;

import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * Created by mweimer on 3/10/14.
 */
public class CodecFileAccumulator<T> implements Accumulator<T> {

  private final Codec<T> codec;
  private final ObjectOutputStream out;

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
      this.out.close();
    } catch (final IOException e) {
      throw new ServiceException(e);
    }
  }
}
