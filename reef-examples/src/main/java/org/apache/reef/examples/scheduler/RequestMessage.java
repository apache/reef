package org.apache.reef.examples.scheduler;


import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * HttpServerShellCmdHandler sends this request message to the driver.
 * The message contains the target(operation) and queryMap(arguments)
 */
public class RequestMessage implements Serializable {
  private String target;
  private Map<String, List<String>> queryMap;
  public static final ObjectSerializableCodec<RequestMessage> CODEC = new ObjectSerializableCodec<>();

  public RequestMessage(String target, Map<String, List<String>> queryMap) {
    this.target = target;
    this.queryMap = queryMap;
  }

  /**
   * @return The target operation to perform : list / status / submit / cancel
   */
  public String getTarget() {
    return target;
  }

  /**
   * @return The arguments indicate {key, value}
   */
  public Map<String, List<String>> getQueryMap() {
    return queryMap;
  }
}
