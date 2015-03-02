/**
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
package org.apache.reef.io.network.impl;

import org.apache.reef.io.network.NetworkEventHandler;
import org.apache.reef.io.network.NetworkLinkListener;
import org.apache.reef.io.network.NetworkServiceParameter;
import org.apache.reef.io.network.exception.NetworkRuntimeException;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Map to correctly match the events and exceptions from NetworkService to
 * their respective NetworkHandler, Codec, and NetworkExceptionHandler.
 */
public final class NetworkPreconfiguredMap {

  private static final Logger LOG = Logger.getLogger(NetworkPreconfiguredMap.class.getName());

  private final Map<Integer, String> eventClassNameMap;
  private final Map<String, Integer> eventCodeMap;
  private final Map<Integer, NetworkEventHandler<?>> handlerMap;
  private final Map<Integer, NetworkLinkListener<?>> linkListenerMap;
  private final Map<Integer, Codec<?>> codecMap;

  @Inject
  public NetworkPreconfiguredMap(
      final @Parameter(NetworkServiceParameter.NetworkEvents.class) Set<String> networkEvents,
      final @Parameter(NetworkServiceParameter.NetworkEventHandlers.class) Set<NetworkEventHandler<?>> networkHandlers,
      final @Parameter(NetworkServiceParameter.Codecs.class) Set<Codec<?>> networkCodecs,
      final @Parameter(NetworkServiceParameter.NetworkLinkListeners.class) Set<NetworkLinkListener<?>> linkListeners) {

    this.eventClassNameMap = new HashMap<>();
    this.handlerMap = new HashMap<>();
    this.linkListenerMap = new HashMap<>();
    this.codecMap = new HashMap<>();
    this.eventCodeMap = new HashMap<>();

    createEventCodeMap(networkEvents);
    insertSetElementsToMap(codecMap, networkCodecs, Codec.class.getSimpleName());
    insertSetElementsToMap(handlerMap, networkHandlers, NetworkEventHandler.class.getSimpleName());
    checkIsCodecAlreadyRegistered(handlerMap, NetworkEventHandler.class.getSimpleName());
    insertSetElementsToMap(linkListenerMap, linkListeners, NetworkLinkListener.class.getSimpleName());
    checkIsCodecAlreadyRegistered(linkListenerMap, NetworkLinkListener.class.getSimpleName());
    putDefaultExceptionHandlers();
  }

  private void createEventCodeMap(final Set<String> networkEvents) {
    final List<String> sortedList = new ArrayList<>(networkEvents.size());
    for (Object eventClassName : networkEvents.toArray()) {
      sortedList.add((String)eventClassName);
    }

    Collections.sort(sortedList);
    for (int i = 0; i < sortedList.size(); i++) {
      eventCodeMap.put(sortedList.get(i), i);
      eventClassNameMap.put(i, sortedList.get(i));
    }
  }

  private <T> void insertSetElementsToMap(final Map<Integer, T> map, final Set<T> set, final String simpleName) {
    for (T obj : set) {
      String eventClassName = getEventClassName(obj.getClass(), simpleName);
      int eventClassNameCode = getEventClassNameCode(eventClassName);
      checkRepetition(map, eventClassNameCode,
          "There are more than one " + simpleName + "s for one event type : " + eventClassName
          + "\nPlease check whether the hash code for " + simpleName + " is same as an already "
          + "configured event type.");

      LOG.log(Level.FINE, simpleName + " for " + eventClassName + " is registered.");
      map.put(eventClassNameCode, obj);
    }
  }

  private <T> void checkIsCodecAlreadyRegistered(final Map<Integer, T> map, final String simpleName) {
    for (T element : map.values()) {
      String eventClassName = getEventClassName(element.getClass(), simpleName);
      if (!codecMap.containsKey(getEventClassNameCode(eventClassName))) {
        throw new NetworkRuntimeException("A Codec must be registered for the event," + eventClassName +
            ", in order to register a " + simpleName + " for it");
      }
    }
  }

  private void checkRepetition(Map<Integer, ?> map, Integer key, String exceptionMessage) {
    if (map.containsKey(key)) {
      throw new NetworkRuntimeException(exceptionMessage);
    }
  }

  private void putDefaultExceptionHandlers() {
    for (Integer key : codecMap.keySet()) {
      if (!linkListenerMap.containsKey(key)) {
        linkListenerMap.put(key, new DefaultNetworkLinkListener());
      }
    }
  }

  private String getEventClassName(Class<?> childClass, String simpleName) {
    for (Type type : childClass.getGenericInterfaces()) {
      final String name = ((Class)((ParameterizedType)type).getRawType()).getSimpleName();
      if (name.equals(simpleName)) {
        return ((Class)(((ParameterizedType)type).getActualTypeArguments()[0])).getName();
      }
    }
    throw new NetworkRuntimeException(childClass.getName() + " is not seems to directly implement " + simpleName);
  }

  /**
   * Returns event class name code
   *
   * @param name event class name
   * @return event class name code
   *
   * @throws org.apache.reef.io.network.exception.NetworkRuntimeException if no correct codec is registered
   */
  public int getEventClassNameCode(String name) {
    if (!eventCodeMap.containsKey(name)) {
      throw new NetworkRuntimeException(
          "You have to register event class " + name + " when you configure network service in driver");
    }
    return eventCodeMap.get(name);
  }

  /**
   * Returns the respective codec for event class name code code
   *
   * @param eventClassNameCode
   * @param <T>
   * @return correct Codec
   *
   * @throws org.apache.reef.io.network.exception.NetworkRuntimeException if no correct codec is registered
   */
  public <T> Codec<T> getCodec(int eventClassNameCode) {
    if (!codecMap.containsKey(eventClassNameCode)) {
      throw new NetworkRuntimeException("No codec for that event class  : " + eventClassNameMap.get(eventClassNameCode));
    }

    return (Codec<T>) codecMap.get(eventClassNameCode);
  }

  /**
   * Returns the respective event handler for event class name code
   *
   * @param eventClassNameCode
   * @param <T>
   * @return correct EventHandler
   *
   * @throws org.apache.reef.io.network.exception.NetworkRuntimeException if no correct event handler is registered
   */
  public <T> EventHandler<T> getEventHandler(int eventClassNameCode) {
    if (!handlerMap.containsKey(eventClassNameCode)) {
      throw new NetworkRuntimeException("No event handler for that event class  : " + eventClassNameMap.get(eventClassNameCode));
    }

    return (EventHandler<T>) handlerMap.get(eventClassNameCode);
  }

  /**
   * Returns respective link listener for event class name
   *
   * @param eventClassName
   * @param <T>
   * @return correct link listener
   *
   * @throws org.apache.reef.io.network.exception.NetworkRuntimeException if no link listener is registered
   */
  public <T> NetworkLinkListener<T> getLinkListener(String eventClassName) {
    return getLinkListener(eventCodeMap.get(eventClassName));
  }

  /**
   * Returns respective link listener for event class name code
   *
   * @param eventClassNameCode
   * @param <T>
   * @return correct link listener
   *
   * @throws org.apache.reef.io.network.exception.NetworkRuntimeException if no link listener is registered
   */
  public <T> NetworkLinkListener<T> getLinkListener(int eventClassNameCode) {
    if (!linkListenerMap.containsKey(eventClassNameCode)) {
      throw new NetworkRuntimeException("No exception handler for that event class  : "
          + eventClassNameMap.get(eventClassNameCode));
    }

    return (NetworkLinkListener<T>) linkListenerMap.get(eventClassNameCode);
  }
}

final class DefaultNetworkLinkListener implements NetworkLinkListener {

  private static final Logger LOG = Logger.getLogger(DefaultNetworkLinkListener.class.getName());

  @Override
  public void onSuccess(Identifier remoteId, List message) {
    if (LOG.isLoggable(Level.FINE)) {
      LOG.log(Level.FINE, "No NetworkLinkListener for {0}. The list of message to {1} is successfully sent : {2}"
          , new Object[]{message.get(0).getClass().getName(), remoteId, message});
    }
  }

  @Override
  public void onException(Throwable cause, Identifier remoteId, List messageList) {
    if (LOG.isLoggable(Level.FINE)) {
      LOG.log(Level.FINE, "No NetworkLinkListener for {0}. The list of message to {1} is failed to be sent. messages : {2}, cause : {3}"
          , new Object[]{messageList.get(0).getClass().getName(), remoteId, messageList, cause});
    }
  }
}