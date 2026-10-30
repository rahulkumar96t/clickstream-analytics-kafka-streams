package io.github.rahulkumar96t.clickstream;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.LockSupport;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Sends synthetic page views to Kafka at a fixed rate. Environment variables (all optional):
 *
 * <pre>
 * BOOTSTRAP_SERVERS  localhost:9092    INPUT_TOPIC      page-views
 * RATE               50  (events/s)    USERS            200
 * DURATION_SECONDS   0   (= forever)   SEED             42
 * LATE_RATIO         0.05              MALFORMED_RATIO  0.02
 * USER_IDLE          PT2M  (pause between a user's sessions)
 * </pre>
 */
public final class LoadGenerator {

  private LoadGenerator() {}

  public static void main(String[] args) {
    Map<String, String> env = System.getenv();
    String topic = env.getOrDefault("INPUT_TOPIC", "page-views");
    int rate = Integer.parseInt(env.getOrDefault("RATE", "50"));
    long durationSeconds = Long.parseLong(env.getOrDefault("DURATION_SECONDS", "0"));
    ClickEventGenerator generator =
        new ClickEventGenerator(
            Long.parseLong(env.getOrDefault("SEED", "42")),
            Integer.parseInt(env.getOrDefault("USERS", "200")),
            Double.parseDouble(env.getOrDefault("LATE_RATIO", "0.05")),
            Double.parseDouble(env.getOrDefault("MALFORMED_RATIO", "0.02")),
            Duration.parse(env.getOrDefault("USER_IDLE", "PT2M")));

    Properties props = new Properties();
    props.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        env.getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092"));
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.LINGER_MS_CONFIG, 5);

    long intervalNanos = 1_000_000_000L / Math.max(1, rate);
    long start = System.nanoTime();
    long deadline = durationSeconds > 0 ? start + Duration.ofSeconds(durationSeconds).toNanos() : 0;
    long sent = 0;
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
      while (deadline == 0 || System.nanoTime() < deadline) {
        ClickEventGenerator.Event event = generator.next(System.currentTimeMillis());
        producer.send(new ProducerRecord<>(topic, event.key(), event.value()));
        sent++;
        if (sent % (10L * rate) == 0) {
          System.out.printf("sent %,d events%n", sent);
        }
        long nextAt = start + sent * intervalNanos;
        LockSupport.parkNanos(nextAt - System.nanoTime());
      }
    }
    System.out.printf("done: sent %,d events%n", sent);
  }
}
