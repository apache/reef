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

  final InjectionFuture<SchedulerDriver> schedulerDriver;

  private String uriSpecification = "reef-example-scheduler";

  private String output = null;


  @Inject
  public HttpServerShellCmdHandler(final InjectionFuture<SchedulerDriver> schedulerDriver) {
    this.schedulerDriver = schedulerDriver;
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
   * The request url is http://{address}:{port}/reef-example-scheduler/v1
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

    final String result;
    switch (target) {
      case "list":
        result = schedulerDriver.get().getList();
        break;
      case "clear":
        result = schedulerDriver.get().clearList();
        break;
      case "status":
        result = schedulerDriver.get().getStatus(queryMap.get("id"));
        break;
      case "submit":
        result = schedulerDriver.get().submitCommands(queryMap.get("cmd"));
        break;
      case "cancel":
        result = schedulerDriver.get().cancelTask(queryMap.get("id"));
        break;
      default:
        result = "Unsupported operation";
    }

    // Send back the response to the http client
    response.getOutputStream().println(result);
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
