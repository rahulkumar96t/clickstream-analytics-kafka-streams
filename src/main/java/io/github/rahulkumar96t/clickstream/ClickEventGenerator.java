package io.github.rahulkumar96t.clickstream;

import io.github.rahulkumar96t.clickstream.model.PageView;
import io.github.rahulkumar96t.clickstream.serde.Json;
import java.time.Duration;
import java.util.List;
import java.util.SplittableRandom;
import java.util.UUID;

/**
 * Produces a synthetic clickstream: users browse in bursts (sessions of a few page views) and then
 * go idle; page popularity is Zipf-like; a configurable share of events is late (event time in the
 * past) or malformed. Deterministic for a given seed.
 */
public final class ClickEventGenerator {

  /** A record to send: key = user id (or {@code null}), value = JSON (or garbage). */
  public record Event(String key, String value) {}

  static final List<String> PAGES =
      List.of(
          "/",
          "/products",
          "/search",
          "/products/1",
          "/products/2",
          "/cart",
          "/products/3",
          "/checkout",
          "/products/4",
          "/about",
          "/products/5",
          "/help");

  private final SplittableRandom random;
  private final double lateRatio;
  private final double malformedRatio;
  private final long idleMs;
  private final long[] idleUntil;
  private final int[] viewsLeft;
  private final double[] cumulativeWeights;

  public ClickEventGenerator(
      long seed, int users, double lateRatio, double malformedRatio, Duration userIdle) {
    this.random = new SplittableRandom(seed);
    this.lateRatio = lateRatio;
    this.malformedRatio = malformedRatio;
    this.idleMs = userIdle.toMillis();
    this.idleUntil = new long[users];
    this.viewsLeft = new int[users];
    this.cumulativeWeights = new double[PAGES.size()];
    double total = 0;
    for (int rank = 0; rank < PAGES.size(); rank++) {
      total += 1.0 / (rank + 1);
      cumulativeWeights[rank] = total;
    }
    for (int rank = 0; rank < PAGES.size(); rank++) {
      cumulativeWeights[rank] /= total;
    }
  }

  public Event next(long nowMillis) {
    if (random.nextDouble() < malformedRatio) {
      return malformed(nowMillis);
    }
    int user = pickActiveUser(nowMillis);
    if (viewsLeft[user] <= 0) {
      viewsLeft[user] = 3 + random.nextInt(13);
    }
    if (--viewsLeft[user] == 0) {
      // Session over: stay away for longer than the session gap.
      idleUntil[user] = nowMillis + idleMs + random.nextLong(idleMs + 1);
    }
    long timestamp =
        random.nextDouble() < lateRatio ? nowMillis - 5_000 - random.nextLong(55_000) : nowMillis;
    String userId = "user-" + user;
    PageView view =
        new PageView(
            new UUID(random.nextLong(), random.nextLong()).toString(),
            userId,
            pickPage(),
            timestamp,
            random.nextInt(4) == 0 ? "https://search.example/?q=shop" : null);
    return new Event(userId, Json.write(view));
  }

  private int pickActiveUser(long now) {
    int user = random.nextInt(idleUntil.length);
    for (int attempt = 0; attempt < 20 && idleUntil[user] > now; attempt++) {
      user = random.nextInt(idleUntil.length);
    }
    return user;
  }

  private String pickPage() {
    double r = random.nextDouble();
    for (int i = 0; i < cumulativeWeights.length; i++) {
      if (r <= cumulativeWeights[i]) {
        return PAGES.get(i);
      }
    }
    return PAGES.getLast();
  }

  private Event malformed(long now) {
    return switch (random.nextInt(4)) {
      case 0 -> new Event(null, "this is not json");
      case 1 -> new Event(null, "{\"page\":\"/\",\"timestamp\":" + now + "}"); // no userId
      case 2 ->
          new Event("user-x", "{\"userId\":\"user-x\",\"page\":\"/\",\"timestamp\":\"soon\"}");
      default -> new Event("user-y", "[1,2,3]");
    };
  }
}
