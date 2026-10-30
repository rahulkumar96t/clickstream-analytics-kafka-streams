package io.github.rahulkumar96t.clickstream.serde;

import io.github.rahulkumar96t.clickstream.model.ParsedPageView;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Source serde for the input topic. Its deserializer never fails, which is what lets malformed
 * records reach the dead-letter branch of the topology instead of tripping the deserialization
 * exception handler.
 */
public final class ParsedPageViewSerde implements Serde<ParsedPageView> {

  @Override
  public Serializer<ParsedPageView> serializer() {
    return (topic, value) ->
        switch (value) {
          case null -> null;
          case ParsedPageView.Valid valid -> Json.bytes(valid.event());
          case ParsedPageView.Invalid invalid -> invalid.raw();
        };
  }

  @Override
  public Deserializer<ParsedPageView> deserializer() {
    return (topic, bytes) -> PageViewParser.parse(bytes);
  }
}
