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

package com.microsoft.reef.examples.hellohttp;

import com.microsoft.reef.util.CommandUtils;
import com.microsoft.reef.webserver.HttpHandler;
import com.microsoft.reef.webserver.RequestParser;
import com.microsoft.tang.InjectionFuture;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.wake.EventHandler;
import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Http Event handler for Shell Command
 */
@Unit
public class HttpServeShellCmdtHandler implements HttpHandler {
    /**
     * Standard Java logger.
     */
    private static final Logger LOG = Logger.getLogger(HttpServeShellCmdtHandler.class.getName());

    /**
     *  ClientMessageHandler
     */
    private final InjectionFuture<HelloDriver.ClientMessageHandler> messageHandler;

    /**
     * uri specification
     */
    private String uriSpecification = "Command";

    /**
     * output for command
     */
    private String cmdOutput = null;

    /**
     * HttpServerDistributedShellEventHandler constructor.
     */
    @Inject
    public HttpServeShellCmdtHandler(final InjectionFuture<HelloDriver.ClientMessageHandler> messageHandler) {
        this.messageHandler = messageHandler;
    }

    /**
     * returns URI specification for the handler
     *
     * @return
     */
    @Override
    public String getUriSpecification() {
        return uriSpecification;
    }

    /**
     * set URI specification
     * @param s
     */
    public void setUriSpecification(final String s) {
        uriSpecification = s;
    }

    /**
     * it is called when receiving a http request
     *
     * @param request
     * @param response
     */
    @Override
    public synchronized void onHttpRequest(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        LOG.log(Level.INFO, "HttpServeShellCmdtHandler in webserver onHttpRequest is called: {0}", request.getRequestURI());
        final RequestParser requestParser = new RequestParser(request);
        Map<String,String> queries = requestParser.getQueryMap();
        final String queryStr = requestParser.getQueryString();

        if (requestParser.getTargetEntity().equalsIgnoreCase("Evaluators")) {
            byte[] b = HelloDriver.CODEC.encode(queryStr);
            LOG.log(Level.INFO, "HttpServeShellCmdtHandler call HelloDriver onCommand(): {0}", queryStr);
            messageHandler.get().onNext(b);

            notify();

            while (cmdOutput == null) {
                try {
                    wait();
                } catch (InterruptedException e) {

                }
            }
            response.getOutputStream().write(cmdOutput.getBytes(Charset.forName("UTF-8")));
            cmdOutput = null;
        } else if (requestParser.getTargetEntity().equalsIgnoreCase("Driver")) {
            String cmdOutput = CommandUtils.runCommand(queryStr);
            response.getOutputStream().write(cmdOutput.getBytes(Charset.forName("UTF-8")));
        }
    }

    /**
     * called after shell command is completed
     * @param message
     */
    public synchronized void onHttpCallback(byte[] message) {
        while (cmdOutput != null) {
            try {
                wait();
            } catch(InterruptedException e) {

            }
        }
        LOG.log(Level.INFO, "HttpServeShellCmdtHandler OnCallback: {0}", HelloDriver.CODEC.decode(message));
        cmdOutput = HelloDriver.CODEC.decode(message);

        notify();
    }

    /**
     * Handler for client to call back
     */
    final class ClientCallBackHandler implements EventHandler<byte[]> {
        @Override
        public void onNext(final byte[] message) {
            HttpServeShellCmdtHandler.this.onHttpCallback(message);
        }
    }
}
