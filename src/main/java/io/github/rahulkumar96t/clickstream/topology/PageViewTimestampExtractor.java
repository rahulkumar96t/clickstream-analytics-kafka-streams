package io.github.rahulkumar96t.clickstream.topology;

import io.github.rahulkumar96t.clickstream.model.ParsedPageView;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Event-time semantics: uses the {@code timestamp} field of a valid page view. The extractor runs
 * after the source deserializer, so it sees the already-parsed value and the JSON is parsed only
 * once. Invalid records keep the broker timestamp (or the partition's stream time) so they are
 * never dropped for a bad timestamp before reaching the dead-letter topic.
 */
public final class PageViewTimestampExtractor implements TimestampExtractor {

  @Override
  public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
    if (record.value() instanceof ParsedPageView.Valid valid) {
      return valid.event().timestamp();
    }
    return record.timestamp() >= 0 ? record.timestamp() : Math.max(0, partitionTime);
  }
}
