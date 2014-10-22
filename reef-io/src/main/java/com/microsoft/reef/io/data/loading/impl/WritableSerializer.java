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
package com.microsoft.reef.io.data.loading.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;

import com.microsoft.reef.io.serialization.Codec;

/**
 * A serializer class that serializes {@link Writable}s
 * into String using the below {@link Codec} that
 * encodes & decodes {@link Writable}s
 * By default this stores the class name in the serialized
 * form so that the specific type can be instantiated on
 * de-serialization. However, this also needs the jobconf
 * to passed in while de-serialization
 */
public class WritableSerializer {
  public static <E extends Writable> String serialize(E writable){
    final WritableCodec<E> writableCodec = new WritableCodec<>();
    return Base64.encodeBase64String(writableCodec.encode(writable));
  }
  
  public static <E extends Writable> E deserialize(String serializedWritable){
    final WritableCodec<E> writableCodec = new WritableCodec<>();
    return writableCodec.decode(Base64.decodeBase64(serializedWritable));
  }
  
  public  static <E extends Writable> E deserialize(String serializedWritable, JobConf jobConf){
    final WritableCodec<E> writableCodec = new WritableCodec<>(jobConf);
    return writableCodec.decode(Base64.decodeBase64(serializedWritable));
  }
  
  static class WritableCodec<E extends Writable> implements Codec<E>{
    private final JobConf jobConf;
    
    public WritableCodec(JobConf jobConf) {
      this.jobConf = jobConf;
    }

    public WritableCodec() {
      this.jobConf = new JobConf();
    }

    @Override
    public E decode(byte[] bytes) {
      final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
      try(DataInputStream dais = new DataInputStream(bais)){
        final String className = dais.readUTF();
        E writable = (E) ReflectionUtils.newInstance(Class.forName(className), jobConf);
        writable.readFields(dais);
        return writable;
      } catch (IOException e) {
        throw new RuntimeException("Could not de-serialize JobConf", e);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Could not instantiate specific writable class", e);
      }
    }

    @Override
    public byte[] encode(E writable) {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try(final DataOutputStream daos = new DataOutputStream(baos)) {
        daos.writeUTF(writable.getClass().getName());
        writable.write(daos);
        return baos.toByteArray();
      } catch (IOException e) {
        throw new RuntimeException("Could not serialize JobConf", e);
      }
    }
  }

}
