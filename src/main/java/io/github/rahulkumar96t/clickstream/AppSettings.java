package io.github.rahulkumar96t.clickstream;

import io.github.rahulkumar96t.clickstream.topology.ClickstreamConfig;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;

/**
 * Runtime settings, read from environment variables (all optional).
 *
 * <pre>
 * BOOTSTRAP_SERVERS   localhost:9092        APPLICATION_ID   clickstream-analytics
 * STATE_DIR           ./state               HTTP_PORT        8080
 * PARTITIONS          3                     REPLICATION      1
 * PROCESSING          exactly_once_v2       (or at_least_once)
 * COUNT_WINDOW        PT1M                  LATENESS         PT30S
 * SESSION_GAP         PT30M                 TOP_N            5
 * </pre>
 */
public record AppSettings(
    String bootstrapServers,
    String applicationId,
    Path stateDir,
    int httpPort,
    int partitions,
    short replication,
    String processingGuarantee,
    ClickstreamConfig topology) {

  public static AppSettings fromEnv(Map<String, String> env) {
    ClickstreamConfig defaults = ClickstreamConfig.defaults();
    ClickstreamConfig topology =
        defaults
            .withWindows(
                duration(env, "COUNT_WINDOW", defaults.countWindow()),
                duration(env, "LATENESS", defaults.lateness()),
                duration(env, "SESSION_GAP", defaults.sessionGap()))
            .withTopN(
                Integer.parseInt(env.getOrDefault("TOP_N", Integer.toString(defaults.topN()))));
    return new AppSettings(
        env.getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092"),
        env.getOrDefault("APPLICATION_ID", "clickstream-analytics"),
        Path.of(env.getOrDefault("STATE_DIR", "state")),
        Integer.parseInt(env.getOrDefault("HTTP_PORT", "8080")),
        Integer.parseInt(env.getOrDefault("PARTITIONS", "3")),
        Short.parseShort(env.getOrDefault("REPLICATION", "1")),
        env.getOrDefault("PROCESSING", StreamsConfig.EXACTLY_ONCE_V2),
        topology);
  }

  public Properties streamsProperties() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toAbsolutePath().toString());
    props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, processingGuarantee);
    props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, (int) replication);
    props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:" + httpPort);
    props.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
    return props;
  }

  private static Duration duration(Map<String, String> env, String name, Duration fallback) {
    String value = env.get(name);
    return value == null ? fallback : Duration.parse(value);
  }
}
