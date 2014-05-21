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
package com.microsoft.wake.remote;


import com.microsoft.tang.annotations.DefaultImplementation;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.Stage;
import com.microsoft.wake.remote.impl.DefaultRemoteManagerImplementation;

/**
 * Represents all remote connections to and from one process to another.
 */
@DefaultImplementation(DefaultRemoteManagerImplementation.class)
public interface RemoteManager extends Stage {
    /**
     * Constructor that takes a Codec<T> 
     */
  
    /**
     * Returns an event handler that can be used to send messages of type T to the
     * given destination.
     *
     * @param <T> 
     * @param destinationIdentifier a destination identifier
     * @param messageType a message class type
     * @return an event handler
     */
    public <T> EventHandler<T> getHandler(final RemoteIdentifier destinationIdentifier, final Class<? extends T> messageType);

    /**
     * Registers the given EventHandler to be invoked when messages of Type T
     * arrive from sourceIdentifier.
     *
     * Calling this method twice overrides the initial registration.
     *
     * @param <T, U extends T> 
     * @param sourceIdentifier a source identifier
     * @param messageType a message class type
     * @param theHandler the event handler
     * @return the subscription that can be used to unsubscribe later
     */
    public <T, U extends T> AutoCloseable registerHandler(final RemoteIdentifier sourceIdentifier, final Class<U> messageType, final EventHandler<T> theHandler);

    /**
     * Registers the given EventHandler to be called for the given message type
     * from any source.
     *
     * If there is an EventHandler registered for this EventType
     *
     * @param <T, U extends T>
     * @param messageType a message class type
     * @param theHandler the event handler
     * @return the subscription that can be used to unsubscribe later
     */
    public <T, U extends T> AutoCloseable registerHandler(final Class<U> messageType, final EventHandler<RemoteMessage<T>> theHandler);

    /**
     * Register an EventHandler that gets called by Wake in the presence of
     * errors. Note that user-level errors that need to cross the network need
     * to be handled as standard messages.
     *
     * @param theHandler the exception event handler
     * @return the subscription that can be used to unsubscribe later
     */
    @Deprecated
    public AutoCloseable registerErrorHandler(final EventHandler<Exception> theHandler);

    /**
     * Access the Identifier of this.
     *
     * @return the Identifier of this.
     */
    public RemoteIdentifier getMyIdentifier();
}
