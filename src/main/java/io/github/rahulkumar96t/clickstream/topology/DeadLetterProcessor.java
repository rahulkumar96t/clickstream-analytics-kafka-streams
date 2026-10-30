package io.github.rahulkumar96t.clickstream.topology;

import io.github.rahulkumar96t.clickstream.model.ParsedPageView;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

/**
 * Turns a rejected record into a dead-letter record: the original key and value bytes, unchanged,
 * plus headers describing why and where it came from, so it can be inspected and replayed.
 */
final class DeadLetterProcessor implements FixedKeyProcessor<String, ParsedPageView, byte[]> {

  static final String REASON_HEADER = "dlq.error.reason";
  static final String TOPIC_HEADER = "dlq.source.topic";
  static final String PARTITION_HEADER = "dlq.source.partition";
  static final String OFFSET_HEADER = "dlq.source.offset";

  private FixedKeyProcessorContext<String, byte[]> context;

  @Override
  public void init(FixedKeyProcessorContext<String, byte[]> context) {
    this.context = context;
  }

  @Override
  public void process(FixedKeyRecord<String, ParsedPageView> record) {
    if (!(record.value() instanceof ParsedPageView.Invalid invalid)) {
      return;
    }
    Headers headers = new RecordHeaders(record.headers().toArray());
    headers.add(REASON_HEADER, utf8(invalid.reason()));
    context
        .recordMetadata()
        .ifPresent(
            source -> {
              headers.add(TOPIC_HEADER, utf8(source.topic()));
              headers.add(PARTITION_HEADER, utf8(Integer.toString(source.partition())));
              headers.add(OFFSET_HEADER, utf8(Long.toString(source.offset())));
            });
    context.forward(record.withValue(invalid.raw()).withHeaders(headers));
  }

  private static byte[] utf8(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }
}
