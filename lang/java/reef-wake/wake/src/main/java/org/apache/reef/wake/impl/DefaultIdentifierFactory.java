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
package org.apache.reef.wake.impl;

import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.exception.RemoteRuntimeException;
import org.apache.reef.wake.remote.impl.SocketRemoteIdentifier;

import javax.inject.Inject;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default remote identifier factory that creates a specific remote identifier
 * from a string representation.
 * <p>
 * A string representation is broken into two parts type and type-specific details separated by "://"
 * A remote identifier implementation should implement a constructor that accepts a string.
 * The factory invokes a proper constructor by reflection.
 */
public class DefaultIdentifierFactory implements IdentifierFactory {

  private static final Logger LOG = Logger.getLogger(DefaultIdentifierFactory.class.getName());

  /** Map between type and remote identifier class. */
  private final Map<String, Class<? extends Identifier>> typeToClazzMap;

  /**
   * Constructs a default remote identifier factory.
   */
  @Inject
  public DefaultIdentifierFactory() {
    typeToClazzMap = new HashMap<>();
    typeToClazzMap.put("socket", SocketRemoteIdentifier.class);
  }

  /**
   * Constructs a default remote identifier factory.
   *
   * @param typeToClazzMap the map of type strings to classes of remote identifiers
   */
  public DefaultIdentifierFactory(final Map<String, Class<? extends Identifier>> typeToClazzMap) {
    this.typeToClazzMap = typeToClazzMap;
  }

  private static final Class<?>[] CONSTRUCTOR_ARG_TYPES = {String.class};

  /**
   * Creates a new remote identifier instance.
   *
   * @param str a string representation
   * @return a remote identifier
   * @throws RemoteRuntimeException
   */
  @Override
  public Identifier getNewInstance(final String str) {

    final int index = str.indexOf("://");
    if (index < 0) {
      throw new RemoteRuntimeException("Invalid remote identifier name: " + str);
    }

    final String type = str.substring(0, index);
    final Class<? extends Identifier> clazz = typeToClazzMap.get(type);

    try {

      final Constructor<? extends Identifier> constructor = clazz.getDeclaredConstructor(CONSTRUCTOR_ARG_TYPES);
      final Object[] args = new Object[] {str.substring(index + 3)};

      final Identifier instance = constructor.newInstance(args);
      LOG.log(Level.FINER, "Created new identifier: {0} for {1}", new Object[] {instance, str});
      return instance;

    } catch (final NoSuchMethodException | SecurityException | InstantiationException
        | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      LOG.log(Level.SEVERE, "Cannot create new identifier for: " + str, e);
      throw new RemoteRuntimeException(e);
    }
  }
}
