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

package com.microsoft.reef.webserver;

import com.microsoft.reef.client.DriverServiceConfiguration;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.evaluator.EvaluatorDescriptor;
import com.microsoft.reef.runtime.common.driver.api.AbstractDriverRuntimeConfiguration;
import com.microsoft.reef.runtime.common.launch.REEFMessageCodec;
import com.microsoft.reef.util.Optional;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Configurations;
import com.microsoft.tang.Injector;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.wake.remote.RemoteConfiguration;
import com.microsoft.wake.time.event.StartTime;
import com.microsoft.wake.time.event.StopTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import javax.inject.Inject;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Test Http Configuration and runtime handlers
 */
public class TestHttpConfiguration {

  private final static Format DATE_TIME_FORMAT = new SimpleDateFormat("yyyy MM dd HH:mm:ss");

  private Injector injector;

  @Before
  public void setUp() throws InjectionException {

    final Configuration httpHandlerConfiguration = HttpHandlerConfiguration.CONF
        .set(HttpHandlerConfiguration.HTTP_HANDLERS, HttpServerReefEventHandler.class)
        .build();

    final Configuration driverConfigurationForHttpServer = DriverServiceConfiguration.CONF
        .set(DriverServiceConfiguration.ON_EVALUATOR_ALLOCATED, ReefEventStateManager.AllocatedEvaluatorStateHandler.class)
        .set(DriverServiceConfiguration.ON_CONTEXT_ACTIVE, ReefEventStateManager.ActiveContextStateHandler.class)
        .set(DriverServiceConfiguration.ON_TASK_RUNNING, ReefEventStateManager.TaskRunningStateHandler.class)
        .set(DriverServiceConfiguration.ON_DRIVER_STARTED, ReefEventStateManager.StartStateHandler.class)
        .set(DriverServiceConfiguration.ON_DRIVER_STOP, ReefEventStateManager.StopStateHandler.class)
        .build();

    final Configuration contextConfig = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(ActiveContext.class, MockActiveContext.class)
        .bindNamedParameter(RemoteConfiguration.ManagerName.class, "REEF_TEST_REMOTE_MANAGER")
        .bindNamedParameter(RemoteConfiguration.MessageCodec.class, REEFMessageCodec.class)
        .bindNamedParameter(AbstractDriverRuntimeConfiguration.JobIdentifier.class, "my job")
        .build();

    final Configuration configuration = Configurations.merge(
        httpHandlerConfiguration, driverConfigurationForHttpServer, contextConfig);

    this.injector = Tang.Factory.getTang().newInjector(configuration);
  }

  @Test
  public void allocatedEvaluatorStateHandlerTest() throws InjectionException {
    final ReefEventStateManager.AllocatedEvaluatorStateHandler h =
        this.injector.getInstance(ReefEventStateManager.AllocatedEvaluatorStateHandler.class);
    Assert.assertNotNull(h);
  }

  @Test
  public void activeContextStateHandlerTest() throws InjectionException {

    final ReefEventStateManager.ActiveContextStateHandler h =
        this.injector.getInstance(ReefEventStateManager.ActiveContextStateHandler.class);
    Assert.assertNotNull(h);

    final MockActiveContext activityContext = injector.getInstance(MockActiveContext.class);
    h.onNext(activityContext);

    final ReefEventStateManager reefEventStateManager =
        this.injector.getInstance(ReefEventStateManager.class);

    final Map<String, ActiveContext> contexts = reefEventStateManager.getContexts();
    Assert.assertEquals(1, contexts.size());

    for (final ActiveContext context : contexts.values()) {
      Assert.assertEquals(activityContext.getId(), context.getId());
    }
  }

  @Test
  public void taskRunningStateHandlerTest() throws InjectionException {
    final ReefEventStateManager.TaskRunningStateHandler h =
        this.injector.getInstance(ReefEventStateManager.TaskRunningStateHandler.class);
    Assert.assertNotNull(h);
  }

  @Test
  public void stopStateHandlerTest() throws InjectionException {

    final ReefEventStateManager.StopStateHandler h =
        this.injector.getInstance(ReefEventStateManager.StopStateHandler.class);
    Assert.assertNotNull(h);

    final StopTime st = new StopTime(new Date().getTime());
    h.onNext(st);

    final ReefEventStateManager reefEventStateManager =
        this.injector.getInstance(ReefEventStateManager.class);

    Assert.assertEquals(reefEventStateManager.getStopTime(), convertTime(st.getTimeStamp()));
  }

  @Test
  public void startStateHandlerTest() throws InjectionException {

    final ReefEventStateManager.StartStateHandler h =
        this.injector.getInstance(ReefEventStateManager.StartStateHandler.class);
    Assert.assertNotNull(h);

    final StartTime st = new StartTime(new Date().getTime());
    h.onNext(st);

    final ReefEventStateManager reefEventStateManager =
        this.injector.getInstance(ReefEventStateManager.class);
    Assert.assertEquals(reefEventStateManager.getStartTime(), convertTime(st.getTimeStamp()));
  }

  private String convertTime(final long time) {
    return DATE_TIME_FORMAT.format(new Date(time)).toString();
  }
}

final class MockActiveContext implements ActiveContext {

  @Inject
  public MockActiveContext() {
  }

  @Override
  public void close() {
    throw new NotImplementedException();
  }

  @Override
  public void submitTask(final Configuration taskConf) {
    throw new NotImplementedException();
  }

  @Override
  public void submitContext(final Configuration contextConfiguration) {
    throw new NotImplementedException();
  }

  @Override
  public void submitContextAndService(final Configuration contextConfiguration, final Configuration serviceConfiguration) {
    throw new NotImplementedException();
  }

  @Override
  public void sendMessage(final byte[] message) {
    throw new NotImplementedException();
  }

  @Override
  public String getEvaluatorId() {
    throw new NotImplementedException();
  }

  @Override
  public Optional<String> getParentId() {
    throw new NotImplementedException();
  }

  @Override
  public EvaluatorDescriptor getEvaluatorDescriptor() {
    throw new NotImplementedException();
  }

  @Override
  public String getId() {
    return "9999";
  }
}
