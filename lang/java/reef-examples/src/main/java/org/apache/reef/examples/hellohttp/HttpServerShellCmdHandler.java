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
package org.apache.reef.examples.hellohttp;

import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.util.CommandUtils;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.webserver.HttpHandler;
import org.apache.reef.webserver.ParsedHttpRequest;

import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Http Event handler for Shell Command.
 */
@Unit
class HttpServerShellCmdHandler implements HttpHandler {
  /**
   * Standard Java logger.
   */
  private static final Logger LOG = Logger.getLogger(HttpServerShellCmdHandler.class.getName());

  private static final int WAIT_TIMEOUT = 10 * 1000;

  private static final int WAIT_TIME = 50;

  /**
   * ClientMessageHandler.
   */
  private final InjectionFuture<HttpShellJobDriver.ClientMessageHandler> messageHandler;

  /**
   * uri specification.
   */
  private String uriSpecification = "Command";

  /**
   * output for command.
   */
  private String cmdOutput = null;

  /**
   * HttpServerShellEventHandler constructor.
   */
  @Inject
  HttpServerShellCmdHandler(final InjectionFuture<HttpShellJobDriver.ClientMessageHandler> messageHandler) {
    this.messageHandler = messageHandler;
  }

  /**
   * returns URI specification for the handler.
   *
   * @return
   */
  @Override
  public String getUriSpecification() {
    return uriSpecification;
  }

  /**
   * set URI specification.
   *
   * @param s
   */
  public void setUriSpecification(final String s) {
    uriSpecification = s;
  }

  /**
   * it is called when receiving a http request.
   *
   * @param parsedHttpRequest
   * @param response
   */
  @Override
  public final synchronized void onHttpRequest(final ParsedHttpRequest parsedHttpRequest,
                                               final HttpServletResponse response)
      throws IOException, ServletException {
    LOG.log(Level.INFO, "HttpServeShellCmdHandler in webserver onHttpRequest is called: {0}",
        parsedHttpRequest.getRequestUri());
    final String queryStr = parsedHttpRequest.getQueryString();

    if (parsedHttpRequest.getTargetEntity().equalsIgnoreCase("Evaluators")) {
      final byte[] b = HttpShellJobDriver.CODEC.encode(queryStr);
      LOG.log(Level.INFO, "HttpServeShellCmdHandler call HelloDriver onCommand(): {0}", queryStr);
      messageHandler.get().onNext(b);

      notify();

      final long endTime = System.currentTimeMillis() + WAIT_TIMEOUT;
      while (cmdOutput == null) {
        final long waitTime = endTime - System.currentTimeMillis();
        if (waitTime <= 0) {
          break;
        }

        try {
          wait(WAIT_TIME);
        } catch (final InterruptedException e) {
          LOG.log(Level.WARNING, "HttpServeShellCmdHandler onHttpRequest InterruptedException: {0}", e);
        }
      }
      if (cmdOutput != null) {
        response.getOutputStream().write(cmdOutput.getBytes(StandardCharsets.UTF_8));
        cmdOutput = null;
      }
    } else if (parsedHttpRequest.getTargetEntity().equalsIgnoreCase("Driver")) {
      final String commandOutput = CommandUtils.runCommand(queryStr);
      response.getOutputStream().write(commandOutput.getBytes(StandardCharsets.UTF_8));
    }
  }

  /**
   * called after shell command is completed.
   *
   * @param message
   */
  public final synchronized void onHttpCallback(final byte[] message) {
    final long endTime = System.currentTimeMillis() + WAIT_TIMEOUT;
    while (cmdOutput != null) {
      final long waitTime = endTime - System.currentTimeMillis();
      if (waitTime <= 0) {
        break;
      }

      try {
        wait(WAIT_TIME);
      } catch (final InterruptedException e) {
        LOG.log(Level.WARNING, "HttpServeShellCmdHandler onHttpCallback InterruptedException: {0}", e);
      }
    }
    LOG.log(Level.INFO, "HttpServeShellCmdHandler OnCallback: {0}", HttpShellJobDriver.CODEC.decode(message));
    cmdOutput = HttpShellJobDriver.CODEC.decode(message);

    notify();
  }

  /**
   * Handler for client to call back.
   */
  final class ClientCallBackHandler implements EventHandler<byte[]> {
    @Override
    public void onNext(final byte[] message) {
      HttpServerShellCmdHandler.this.onHttpCallback(message);
    }
  }
}
