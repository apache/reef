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
import java.util.List;
import java.util.Map;
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
   *
   * APIs
   *   /list              to get the status list for all tasks
   *   /status?id={id}    to query the status of such a task, given id
   *   /submit?cmd={cmd}  to submit a Task, which returns its id
   *   /cancel?id={id}    to cancel the task's execution
   *   /clear             to clear the waiting queue
   */
  @Override
  public void onHttpRequest(ParsedHttpRequest request, HttpServletResponse response) throws IOException, ServletException {
    final String target = request.getTargetEntity().toLowerCase();
    final Map<String, List<String>> queryMap = request.getQueryMap();

    sendMessage(target, queryMap);

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
      response.getOutputStream().println(output);
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

  /**
   * Send message to the Driver. The message contains command and arguments
   * @param target Operation to execute (e.g. submit / cancel / getstatus)
   * @param queryMap Arguments for the command
   */
  private void sendMessage(final String target, final Map<String, List<String>> queryMap) {
    final RequestMessage message = new RequestMessage(target, queryMap);
    messageHandler.get().onNext(RequestMessage.CODEC.encode(message));
  }
}
