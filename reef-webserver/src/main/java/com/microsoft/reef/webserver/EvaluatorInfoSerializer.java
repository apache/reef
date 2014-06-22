package com.microsoft.reef.webserver;

import com.microsoft.reef.driver.evaluator.EvaluatorDescriptor;
import com.microsoft.tang.annotations.DefaultImplementation;

import java.util.List;
import java.util.Map;

/**
 * interface for EvaluatorInfoSerializer
 */
@DefaultImplementation(AvroEvaluatorInfoSerializer.class)
public interface EvaluatorInfoSerializer {
  /**
   * Build AvroEvaluatorsInfo object from raw data
   * @param ids
   * @param evaluators
   * @return
   */
  public AvroEvaluatorsInfo toAvro(final List<String> ids, final Map<String, EvaluatorDescriptor> evaluators);

  /**
   * Convert AvroEvaluatorsInfo object to JSon string
   * @param avroEvaluatorsInfo
   * @return
   */
  public String toString(final AvroEvaluatorsInfo avroEvaluatorsInfo);
}
