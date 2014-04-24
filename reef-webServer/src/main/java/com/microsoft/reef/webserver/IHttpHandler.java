package com.microsoft.reef.webserver;

/**
 * Created with IntelliJ IDEA.
 * User: juwang
 * Date: 4/23/14
 * Time: 9:42 AM
 * To change this template use File | Settings | File Templates.
 */
public interface IHttpHandler {
    String getUriSpecification();
    void onHttpRequest(JobDriverHttpRequest request, JobDriverHttpResponse response);
}