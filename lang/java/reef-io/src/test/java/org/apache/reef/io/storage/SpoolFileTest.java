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
package org.apache.reef.io.storage;

import org.apache.reef.exception.evaluator.ServiceException;
import org.apache.reef.io.Accumulable;
import org.apache.reef.io.Accumulator;
import org.apache.reef.io.Spool;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.io.serialization.Deserializer;
import org.apache.reef.io.serialization.Serializer;
import org.apache.reef.io.storage.local.CodecFileAccumulable;
import org.apache.reef.io.storage.local.CodecFileIterable;
import org.apache.reef.io.storage.local.LocalStorageService;
import org.apache.reef.io.storage.local.SerializerFileSpool;
import org.apache.reef.io.storage.ram.RamSpool;
import org.apache.reef.io.storage.ram.RamStorageService;
import org.apache.reef.io.storage.util.IntegerCodec;
import org.apache.reef.tang.ConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

public class SpoolFileTest {
  private final Serializer<Integer, OutputStream> serializer = new Serializer<Integer, OutputStream>() {
    @Override
    public Accumulable<Integer> create(final OutputStream out) {
      return new Accumulable<Integer>() {

        @Override
        public Accumulator<Integer> accumulator() {
          return new Accumulator<Integer>() {

            @Override
            public void add(final Integer datum) {
              try {
                final int d = datum;
                out.write(new byte[]{(byte) (d >>> 24), (byte) (d >>> 16),
                    (byte) (d >>> 8), (byte) d});
              } catch (final IOException e) {
                throw new IllegalStateException(e);
              }
            }

            @Override
            public void close() {
              try {
                out.flush();
              } catch (final IOException e) {
                throw new IllegalStateException(e);
              }
            }
          };
        }
      };
    }
  };
  private final Deserializer<Integer, InputStream> deserializer = new Deserializer<Integer, InputStream>() {
    @Override
    public Iterable<Integer> create(final InputStream in) {
      return new Iterable<Integer>() {
        @Override
        public Iterator<Integer> iterator() {
          final Iterator<Integer> it = new Iterator<Integer>() {
            private final byte[] inb = new byte[4];
            private Integer nextInt;

            @Override
            public boolean hasNext() {
              return nextInt != null;
            }

            private void prime() {
              final int read;
              try {
                read = in.read(inb);
              } catch (final IOException e) {
                throw new IllegalStateException(e);
              }
              if (read != 4) {
                nextInt = null;
              } else {
                nextInt = ((inb[0] & 0xFF) << 24) + ((inb[1] & 0xFF) << 16)
                    + ((inb[2] & 0xFF) << 8) + (inb[3] & 0xFF);
              }

            }

            @Override
            public Integer next() {
              final Integer ret = nextInt;
              prime();
              return ret;
            }

            @Override
            public void remove() {
              throw new UnsupportedOperationException();
            }
          };
          it.next(); // calls prime
          return it;
        }
      };
    }
  };

  @Test
  public void testRam() throws BindException, InjectionException, ServiceException, IOException {
    final Tang t = Tang.Factory.getTang();
    final ConfigurationBuilder configurationBuilderOne = t.newConfigurationBuilder(RamConf.CONF.build());

    final AvroConfigurationSerializer avroSerializer = new AvroConfigurationSerializer();
    final String serializedConfiguration = avroSerializer.toString(configurationBuilderOne.build());
    final ConfigurationBuilder configurationBuilderTwo =
        t.newConfigurationBuilder(avroSerializer.fromString(serializedConfiguration));

    @SuppressWarnings("unchecked")
    final Spool<Integer> f = (Spool<Integer>) t.newInjector(configurationBuilderTwo.build()).getInstance(
        Spool.class);
    test(f);
  }

  @Test
  public void testFile() throws ServiceException {
    final LocalStorageService service = new LocalStorageService("spoolTest", "file");
    final Spool<Integer> f = new SerializerFileSpool<>(service, serializer,
        deserializer);
    test(f);
    service.getScratchSpace().delete();
  }

  @Test
  public void testInterop() throws ServiceException {
    final LocalStorageService service = new LocalStorageService("spoolTest", "file");
    final Codec<Integer> c = new IntegerCodec();


    final CodecFileAccumulable<Integer, Codec<Integer>> f = new CodecFileAccumulable<>(
        service, c);
    final CodecFileIterable<Integer, Codec<Integer>> g = new CodecFileIterable<>(
        new File(f.getName()), c);
    test(f, g);
    service.getScratchSpace().delete();
  }

  protected void test(final Spool<Integer> f) throws ServiceException {
    test(f, f);
  }

  protected void test(final Accumulable<Integer> f, final Iterable<Integer> g) throws ServiceException {

    try (Accumulator<Integer> acc = f.accumulator()) {
      for (int i = 0; i < 1000; i++) {
        acc.add(i);
      }
    }
    int i = 0;
    for (final int j : g) {
      Assert.assertEquals(i, j);
      i++;
    }
    final Iterator<Integer> itA = g.iterator();
    final Iterator<Integer> itB = g.iterator();

    for (i = 0; i < 1000; i++) {
      Assert.assertEquals((int) itA.next(), i);
      Assert.assertEquals((int) itB.next(), i);
    }
    Assert.assertFalse(itA.hasNext());
    Assert.assertFalse(itB.hasNext());
  }

  public static final class RamConf extends ConfigurationModuleBuilder {
    public static final ConfigurationModule CONF = new RamConf()
        .bindImplementation(RamStorageService.class, RamStorageService.class)
        .bindImplementation(Spool.class, RamSpool.class)
        .build();
  }
}
