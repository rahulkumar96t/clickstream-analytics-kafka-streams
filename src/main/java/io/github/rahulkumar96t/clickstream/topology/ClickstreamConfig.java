package io.github.rahulkumar96t.clickstream.topology;

import java.time.Duration;
import java.util.List;

/**
 * Topic names and window parameters of the topology.
 *
 * @param countWindow size of the tumbling windows for page counts and top-N
 * @param lateness grace period: how late (in event time) a view may arrive and still be counted
 * @param sessionGap inactivity gap that ends a user session
 * @param topN number of pages ranked per window
 * @param countsRetention how long window counts stay queryable
 * @param sessionsRetention how long sessions stay queryable
 * @param topPagesRetention how long top-N results stay queryable
 */
public record ClickstreamConfig(
    String inputTopic,
    String pageCountsTopic,
    String topPagesTopic,
    String sessionsTopic,
    String deadLetterTopic,
    Duration countWindow,
    Duration lateness,
    Duration sessionGap,
    int topN,
    Duration countsRetention,
    Duration sessionsRetention,
    Duration topPagesRetention) {

  public ClickstreamConfig {
    if (topN < 1) {
      throw new IllegalArgumentException("topN must be >= 1");
    }
    if (countsRetention.compareTo(countWindow.plus(lateness)) < 0) {
      throw new IllegalArgumentException("countsRetention must cover countWindow + lateness");
    }
    if (sessionsRetention.compareTo(sessionGap.plus(lateness)) < 0) {
      throw new IllegalArgumentException("sessionsRetention must cover sessionGap + lateness");
    }
  }

  /** One-minute windows, 30 s of lateness, 30-minute sessions, top 5. */
  public static ClickstreamConfig defaults() {
    return new ClickstreamConfig(
        "page-views",
        "page-view-counts",
        "top-pages",
        "user-sessions",
        "page-views-dlq",
        Duration.ofMinutes(1),
        Duration.ofSeconds(30),
        Duration.ofMinutes(30),
        5,
        Duration.ofHours(1),
        Duration.ofHours(24),
        Duration.ofHours(24));
  }

  public ClickstreamConfig withTopN(int n) {
    return new ClickstreamConfig(
        inputTopic,
        pageCountsTopic,
        topPagesTopic,
        sessionsTopic,
        deadLetterTopic,
        countWindow,
        lateness,
        sessionGap,
        n,
        countsRetention,
        sessionsRetention,
        topPagesRetention);
  }

  public ClickstreamConfig withWindows(
      Duration countWindow, Duration lateness, Duration sessionGap) {
    return new ClickstreamConfig(
        inputTopic,
        pageCountsTopic,
        topPagesTopic,
        sessionsTopic,
        deadLetterTopic,
        countWindow,
        lateness,
        sessionGap,
        topN,
        max(countsRetention, countWindow.plus(lateness)),
        max(sessionsRetention, sessionGap.plus(lateness)),
        topPagesRetention);
  }

  /** Every topic the application reads or writes (internal topics excluded). */
  public List<String> topics() {
    return List.of(inputTopic, pageCountsTopic, topPagesTopic, sessionsTopic, deadLetterTopic);
  }

  private static Duration max(Duration a, Duration b) {
    return a.compareTo(b) >= 0 ? a : b;
  }
}
