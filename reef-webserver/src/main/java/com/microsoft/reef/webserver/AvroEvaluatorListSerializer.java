package com.microsoft.reef.webserver;

import com.microsoft.reef.driver.evaluator.EvaluatorDescriptor;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.formats.avro.AvroConfiguration;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Serializer for Evaluator list
 */
public class AvroEvaluatorListSerializer {
  /**
   * The Charset used for the JSON encoding.
   * <p/>
   * Copied from <code>org.apache.avro.io.JsonDecoder.CHARSET</code>
   */
  private static final String JSON_CHARSET = "ISO-8859-1";

  public AvroEvaluatorList toAvro(final Map<String, EvaluatorDescriptor> evaluatorMap, final int totalEvaluators) {
    final List<AvroEvaluatorEntry> EvaluatorEntities = new ArrayList<>();
    for (final Map.Entry<String, EvaluatorDescriptor> entry : evaluatorMap.entrySet()) {
      final String key = entry.getKey();
      final EvaluatorDescriptor descriptor = entry.getValue();
      EvaluatorEntities.add(AvroEvaluatorEntry.newBuilder()
          .setId(key)
          .setName(descriptor.getNodeDescriptor().getName())
          .build());
    }

    return AvroEvaluatorList.newBuilder()
        .setEvaluators(EvaluatorEntities)
        .setTotal(totalEvaluators)
        .build();
  }

  public String toString(final AvroEvaluatorList avroEvaluatorList) {
    final DatumWriter<AvroEvaluatorList> evaluatorWriter = new SpecificDatumWriter<>(AvroEvaluatorList.class);
    final String result;
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      final JsonEncoder encoder = EncoderFactory.get().jsonEncoder(avroEvaluatorList.getSchema(), out);
      evaluatorWriter.write(avroEvaluatorList, encoder);
      encoder.flush();
      out.flush();
      result = out.toString(JSON_CHARSET);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    return result;
  }
}
