package org.apache.reef.examples.scheduler;

import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.webserver.HttpHandler;
import org.apache.reef.webserver.ParsedHttpRequest;

import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Receive HttpRequest so that it can handle the command list
 */
@Unit
public class HttpServerShellCmdHandler implements HttpHandler {
  private static final Logger LOG = Logger.getLogger(HttpServerShellCmdHandler.class.getName());

  final InjectionFuture<SchedulerDriver.CommandRequestHandler> messageHandler;

  private String uriSpecification = "Reef";

  private String output = null;

  @Inject
  public HttpServerShellCmdHandler(final InjectionFuture<SchedulerDriver.CommandRequestHandler> messageHandler) {
    this.messageHandler = messageHandler;
  }

  @Override
  public String getUriSpecification() {
    return uriSpecification;
  }

  @Override
  public void setUriSpecification(String s) {
    uriSpecification = s;
  }

  /**
   * HttpRequest handler. You must specify UriSpecification and REST API version.
   * The request url is http://{address}:{port}/Reef/v1
   */
  @Override
  public void onHttpRequest(ParsedHttpRequest request, HttpServletResponse response) throws IOException, ServletException {
    // Send the request message to the handler of driver
    messageHandler.get().onNext(request.getInputStream());

    /*
     * Wait until the response arrives. The response is how many commands the Driver received
     * and how many tasks reside in the queue
     */
    try {
      while (output == null) {
        synchronized (this) {
          this.wait();
        }
      }
      // Send back the response to the http client
      response.getOutputStream().print(output);
      output = null;
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  final class CallbackHandler implements EventHandler<byte[]> {
    @Override
    public void onNext(byte[] message) {
      // When the response arrives from the driver, wake up the handler to send the response
      output = SchedulerDriver.CODEC.decode(message);
      LOG.log(Level.INFO, output);
      synchronized (this) {
        this.notify();
      }
    }
  }
}
