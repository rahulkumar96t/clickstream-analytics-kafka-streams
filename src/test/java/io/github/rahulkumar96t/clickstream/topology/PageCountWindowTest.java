package io.github.rahulkumar96t.clickstream.topology;

import static io.github.rahulkumar96t.clickstream.topology.TopologyHarness.ms;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.rahulkumar96t.clickstream.model.PageCount;
import java.nio.file.Path;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** One-minute tumbling windows with 30 s of grace, emitted once when the window closes. */
class PageCountWindowTest {

  @TempDir Path stateDir;
  private TopologyHarness h;

  @BeforeEach
  void setUp() {
    h = new TopologyHarness(ClickstreamConfig.defaults(), stateDir);
  }

  @AfterEach
  void tearDown() {
    h.close();
  }

  private static PageCount count(String page, String windowStart, String windowEnd, long n) {
    return new PageCount(page, ms(windowStart), ms(windowEnd), n);
  }

  @Test
  void countsViewsPerPageInOneMinuteWindowsAndEmitsWhenTheWindowCloses() {
    h.view("u1", "/home", "10:00:05");
    h.view("u2", "/home", "10:00:20");
    h.view("u1", "/cart", "10:00:59.999");
    h.view("u3", "/home", "10:01:00"); // first instant of the next window

    // Window [10:00, 10:01) closes once stream time reaches 10:01:00 + 30 s grace.
    h.view("u4", "/about", "10:01:29.999");
    assertThat(h.pageCounts.isEmpty()).isTrue();

    h.view("u4", "/about", "10:01:30");
    assertThat(h.pageCounts.readValuesToList())
        .containsExactlyInAnyOrder(
            count("/home", "10:00:00", "10:01:00", 2), count("/cart", "10:00:00", "10:01:00", 1));
  }

  @Test
  void lateEventsWithinTheGracePeriodAreCounted() {
    h.view("u1", "/home", "10:00:10");
    h.view("u1", "/home", "10:01:20"); // stream time 10:01:20, window 10:00 still open
    h.view("u2", "/home", "10:00:50"); // 30 s late (event time), within grace

    h.view("u3", "/other", "10:02:30"); // closes 10:00 and 10:01
    assertThat(h.pageCounts.readValuesToList())
        .contains(
            count("/home", "10:00:00", "10:01:00", 2), count("/home", "10:01:00", "10:02:00", 1));
  }

  @Test
  void eventsAfterTheGracePeriodAreDroppedAndCounted() {
    h.view("u1", "/home", "10:00:10");
    h.view("u1", "/next", "10:01:40"); // closes window 10:00 (10:01:00 + 30 s <= 10:01:40)
    assertThat(h.pageCounts.readValuesToList())
        .containsExactly(count("/home", "10:00:00", "10:01:00", 1));

    h.view("u2", "/home", "10:00:59"); // too late: its window is closed
    h.view("u3", "/next", "10:03:00");

    assertThat(h.pageCounts.readValuesToList())
        .containsExactly(count("/next", "10:01:00", "10:02:00", 1)); // no second /home result
    assertThat(droppedRecords()).isEqualTo(1.0);
  }

  @Test
  void outOfOrderEventsInsideAWindowAreCountedOnce() {
    h.view("u1", "/p", "10:00:40");
    h.view("u2", "/p", "10:00:10");
    h.view("u3", "/p", "10:00:30");
    h.view("u4", "/p", "10:00:01");
    h.view("u5", "/x", "10:05:00");

    assertThat(h.pageCounts.readKeyValuesToList())
        .singleElement()
        .satisfies(
            kv -> {
              assertThat(kv.key).isEqualTo("/p");
              assertThat(kv.value).isEqualTo(count("/p", "10:00:00", "10:01:00", 4));
            });
  }

  @Test
  void liveCountsAreQueryableBeforeTheWindowCloses() {
    h.view("u1", "/home", "10:00:05");
    h.view("u2", "/home", "10:00:06");

    var store = h.driver.<String, Long>getWindowStore(ClickstreamTopology.PAGE_COUNTS_STORE);
    assertThat(store.fetch("/home", ms("10:00:00"))).isEqualTo(2L);
    assertThat(h.pageCounts.isEmpty()).isTrue();
  }

  private double droppedRecords() {
    double total = 0;
    for (var entry : h.driver.metrics().entrySet()) {
      MetricName name = entry.getKey();
      Metric metric = entry.getValue();
      if (name.name().equals("dropped-records-total")
          && name.group().equals("stream-task-metrics")) {
        total += (Double) metric.metricValue();
      }
    }
    return total;
  }
}
