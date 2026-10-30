package io.github.rahulkumar96t.clickstream.topology;

import io.github.rahulkumar96t.clickstream.model.PageCount;
import io.github.rahulkumar96t.clickstream.model.TopPages;
import io.github.rahulkumar96t.clickstream.model.UserSession;
import io.github.rahulkumar96t.clickstream.serde.Json;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;

/** Runs the real topology in a {@link TopologyTestDriver}: no broker, fully deterministic. */
public final class TopologyHarness implements AutoCloseable {

  public final ClickstreamConfig config;
  public final TopologyTestDriver driver;
  public final TestInputTopic<String, String> input;
  public final TestOutputTopic<String, PageCount> pageCounts;
  public final TestOutputTopic<String, TopPages> topPages;
  public final TestOutputTopic<String, UserSession> sessions;
  public final TestOutputTopic<String, byte[]> deadLetters;

  public TopologyHarness(ClickstreamConfig config, Path stateDir) {
    this.config = config;
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "clickstream-test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
    props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
    props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
    // Emit-on-window-close checks for closed windows at most once per second of wall-clock time
    // by default; the test driver's clock does not move, so check on every record instead.
    props.put(StreamsConfig.InternalConfig.EMIT_INTERVAL_MS_KSTREAMS_WINDOWED_AGGREGATION, 0L);
    this.driver = new TopologyTestDriver(ClickstreamTopology.build(config), props, t("00:00:00"));
    this.input =
        driver.createInputTopic(
            config.inputTopic(), Serdes.String().serializer(), Serdes.String().serializer());
    this.pageCounts =
        driver.createOutputTopic(
            config.pageCountsTopic(),
            Serdes.String().deserializer(),
            Json.serde(PageCount.class).deserializer());
    this.topPages =
        driver.createOutputTopic(
            config.topPagesTopic(),
            Serdes.String().deserializer(),
            Json.serde(TopPages.class).deserializer());
    this.sessions =
        driver.createOutputTopic(
            config.sessionsTopic(),
            Serdes.String().deserializer(),
            Json.serde(UserSession.class).deserializer());
    this.deadLetters =
        driver.createOutputTopic(
            config.deadLetterTopic(),
            Serdes.String().deserializer(),
            Serdes.ByteArray().deserializer());
  }

  /** An instant on the test day, e.g. {@code t("10:00:30")}. */
  public static Instant t(String time) {
    return Instant.parse("2026-01-01T" + time + "Z");
  }

  public static long ms(String time) {
    return t(time).toEpochMilli();
  }

  /** Sends a valid page view whose event time (and record timestamp) is {@code time}. */
  public void view(String user, String page, String time) {
    Instant at = t(time);
    String json =
        Json.write(
            Map.of("userId", user, "page", page, "timestamp", at.toEpochMilli(), "eventId", "e"));
    input.pipeInput(user, json, at);
  }

  /** Sends an arbitrary raw value. */
  public void raw(String key, String value) {
    input.pipeInput(key, value, t("00:00:00"));
  }

  @Override
  public void close() {
    driver.close();
  }
}
