/**
 * Copyright (C) 2013 Microsoft Corporation
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
package com.microsoft.reef.io.network.util;

import com.microsoft.wake.remote.Codec;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class ListCodec<T> implements Codec<List<T>> {

  /**
   * @param args
   */
  public static void main(String[] args) {
    String[] strArr = {"One", "Two", "Three", "Four", "Five"};
    List<String> arrList = Arrays.asList(strArr);
    System.out.println(arrList);
    ListCodec<String> lstCodec = new ListCodec<>(new StringCodec());
    byte[] bytes = lstCodec.encode(arrList);
    System.out.println(lstCodec.decode(bytes));
  }

  Codec<T> codec;


  public ListCodec(Codec<T> codec) {
    super();
    this.codec = codec;
  }

  @Override
  public byte[] encode(List<T> arg0) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream daos = new DataOutputStream(baos);
    try {
      for (T t : arg0) {
        byte[] tBytes = codec.encode(t);

        daos.writeInt(tBytes.length);
        daos.write(tBytes);

      }
      daos.close();
    } catch (IOException e) {
      throw new RuntimeException(e.getCause());
    }
    return baos.toByteArray();
  }

  @Override
  public List<T> decode(byte[] arg0) {
    List<T> result = new ArrayList<>();
    ByteArrayInputStream bais = new ByteArrayInputStream(arg0);
    DataInputStream dais = new DataInputStream(bais);
    try {
      while (dais.available() > 0) {
        int length = dais.readInt();
        byte[] tBytes = new byte[length];
        dais.readFully(tBytes);
        T t = codec.decode(tBytes);
        result.add(t);
      }
    } catch (IOException e) {
      throw new RuntimeException(e.getCause());
    }
    return result;
  }

}
