package com.microsoft.reef.webserver;

import com.microsoft.reef.driver.evaluator.EvaluatorDescriptor;
import com.microsoft.tang.annotations.DefaultImplementation;

import java.util.Map;

/**
 * interface for EvaluatorListSerializer
 */
@DefaultImplementation(AvroEvaluatorListSerializer.class)
public interface EvaluatorListSerializer {
  /**
   * Build AvroEvaluatorList object
   * @param evaluatorMap
   * @param totalEvaluators
   * @param startTime
   * @return
   */
  public AvroEvaluatorList toAvro(final Map<String, EvaluatorDescriptor> evaluatorMap, final int totalEvaluators, final String startTime);

  /**
   * Convert AvroEvaluatorList to JSon string
   * @param avroEvaluatorList
   * @return
   */
  public String toString(final AvroEvaluatorList avroEvaluatorList);
}
