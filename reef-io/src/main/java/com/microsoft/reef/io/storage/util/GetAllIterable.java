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
package com.microsoft.reef.io.storage.util;

import com.microsoft.reef.io.ExternalMap;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

public class GetAllIterable<T> implements
    Iterable<Map.Entry<CharSequence, T>> {
  private final Set<? extends CharSequence> keys;
  private final ExternalMap<T> map;

  public GetAllIterable(Set<? extends CharSequence> keys, ExternalMap<T> map) {
    this.keys = keys;
    this.map = map;
  }
  @Override
  public Iterator<Map.Entry<CharSequence,T>> iterator() {
    final Iterator<? extends CharSequence> k = keys.iterator();
    return new Iterator<Map.Entry<CharSequence,T>>() {
      CharSequence lastKey = null;
      CharSequence curKey = findNextKey();
      private CharSequence findNextKey() {
        while (k.hasNext()) {
          CharSequence next = k.next();
          if (map.containsKey(next)) {
            return next;
          }
        }
        return null;
      }
  
      @Override
      public boolean hasNext() {
        return curKey != null;
      }
  
      @Override
      public Map.Entry<CharSequence, T> next() {
        final CharSequence key = curKey;
        curKey = findNextKey();
        lastKey = key;
        if (key == null)
          throw new NoSuchElementException();
  
        final T v = map.get(key);
  
        return new Map.Entry<CharSequence, T>() {
          @Override
          public CharSequence getKey() {
            return key;
          }
  
          @Override
          public T getValue() {
            return v;
          }
  
          @Override
          public T setValue(T v) {
            throw new UnsupportedOperationException(
                "No support for mutating values via iterator");
          }
        };
      }
      @Override
      public void remove() {
        map.remove(lastKey);
      }
    };
  }
}