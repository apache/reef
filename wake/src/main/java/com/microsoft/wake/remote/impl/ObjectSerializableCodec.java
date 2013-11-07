/**
 * Copyright 2013 Microsoft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.wake.remote.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import com.microsoft.wake.remote.Codec;
import com.microsoft.wake.remote.exception.RemoteRuntimeException;

/**
 * Codec that uses Java serialization
 *
 * @param <T>
 */
public class ObjectSerializableCodec<T> implements Codec<T> {
  
  /**
   * Encodes the object to bytes
   * 
   * @param obj the object
   * @return bytes
   * @throws RemoteRuntimeException
   */
  @Override
  public byte[] encode(T obj) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutput out = null;
    try {
      out = new ObjectOutputStream(bos);   
      out.writeObject(obj);
      return bos.toByteArray();
    } catch (IOException e) {
      throw new RemoteRuntimeException(e);
    } finally {
      try {
        if (out != null) out.close();
        bos.close();
      } catch (IOException e) {
        throw new RemoteRuntimeException(e);
      }
    }
  }

  /**
   * Decodes an object from the bytes
   * 
   * @param buf the bytes
   * @return an object
   * @throws RemoteRuntimeException
   */
  @SuppressWarnings("unchecked")
  @Override
  public T decode(byte[] buf) {
    ByteArrayInputStream bis = new ByteArrayInputStream(buf);
    ObjectInput in = null;
    try {
      in = new ObjectInputStream(bis);
      return (T)in.readObject();
    } catch (ClassNotFoundException e) {
      throw new RemoteRuntimeException(e);
    } catch (IOException e) {
      throw new RemoteRuntimeException(e);
    } finally {
      try {
        bis.close();
        if (in != null) in.close();
      } catch (IOException e) {
        throw new RemoteRuntimeException(e);
      }
    }
  }
}
