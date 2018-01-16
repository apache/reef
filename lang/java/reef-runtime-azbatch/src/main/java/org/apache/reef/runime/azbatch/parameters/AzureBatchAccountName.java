package org.apache.reef.runime.azbatch.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "The Azure Batch Account URL")
public class AzureBatchAccountName implements Name<String> {
}
