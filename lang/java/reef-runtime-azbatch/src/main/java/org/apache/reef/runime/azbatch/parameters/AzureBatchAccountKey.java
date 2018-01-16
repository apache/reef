package org.apache.reef.runime.azbatch.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "The Azure Batch account key")
public class AzureBatchAccountKey implements Name<String> {
}
