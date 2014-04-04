package com.microsoft.reef.runtime.common.driver.resourcemanager;

import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.proto.DriverRuntimeProtocol;
import com.microsoft.reef.runtime.common.driver.catalog.ResourceCatalogImpl;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;

/**
 * A NodeDescriptorProto defines a new node in the cluster. We should add this to the resource catalog
 * so that clients can make resource requests against it.
 */
@Private
public final class NodeDescriptorHandler implements EventHandler<DriverRuntimeProtocol.NodeDescriptorProto> {
  private final ResourceCatalogImpl resourceCatalog;

  @Inject
  public NodeDescriptorHandler(ResourceCatalogImpl resourceCatalog) {
    this.resourceCatalog = resourceCatalog;
  }

  @Override
  public void onNext(final DriverRuntimeProtocol.NodeDescriptorProto value) {
    this.resourceCatalog.handle(value);
  }
}