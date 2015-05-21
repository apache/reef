package org.apache.reef.io.network;

import org.apache.reef.wake.EventHandler;

/**
 * EventHandler that handles wrapped NetworkEvents
 */
public interface NetworkEventHandler<T> extends EventHandler<NetworkEvent<T>> {
}
