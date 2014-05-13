package com.microsoft.reef.examples.hellohttp;

import com.microsoft.reef.driver.parameters.ClientMessageHandlers;
import com.microsoft.reef.webserver.HttpHandler;
import com.microsoft.reef.webserver.RequestParser;
import com.microsoft.tang.InjectionFuture;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Event handler for Shell Command
 */
@Unit
public class HttpServeShellCmdtHandler implements HttpHandler {
    /**
     * Standard Java logger.
     */
    private static final Logger LOG = Logger.getLogger(HttpServeShellCmdtHandler.class.getName());

    //private final HelloDriver helloDriver;
    private final InjectionFuture<HelloDriver.ClientMessageHandler> messageHandler;


    /**
     * HttpServerDistributedShellEventHandler constructor.
     */
    @Inject
    public HttpServeShellCmdtHandler(InjectionFuture<HelloDriver.ClientMessageHandler> messageHandler) {
        this.messageHandler = messageHandler;
    }

    /**
     * returns URI specification for the handler
     *
     * @return
     */
    @Override
    public String getUriSpecification() {
        return "Command";
    }

    private String cmdOutput = null;
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
            String cmdOutput = CommandUtility.runCommand(queryStr);
            response.getOutputStream().write(cmdOutput.getBytes(Charset.forName("UTF-8")));
        }
    }

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

    final class ClientCallBackHandler implements EventHandler<byte[]> {
        @Override
        public void onNext(final byte[] message) {
            HttpServeShellCmdtHandler.this.onHttpCallback(message);
        }

    }
}
