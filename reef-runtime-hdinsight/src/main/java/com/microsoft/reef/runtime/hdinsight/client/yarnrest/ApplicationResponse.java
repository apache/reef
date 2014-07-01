package com.microsoft.reef.runtime.hdinsight.client.yarnrest;

import java.util.List;
import java.util.Map;

/**
 * Created by marku_000 on 2014-06-30.
 */
public class ApplicationResponse {

  private Map<String, List<ApplicationState>> apps;

  public Map<String, List<ApplicationState>> getApps() {
    return apps;
  }

  public void setApps(Map<String, List<ApplicationState>> apps) {
    this.apps = apps;
  }

  public List<ApplicationState> getApplicationStates() {
    return apps.get("app");
  }


}
