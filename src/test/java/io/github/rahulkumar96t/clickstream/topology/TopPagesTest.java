package io.github.rahulkumar96t.clickstream.topology;

import static io.github.rahulkumar96t.clickstream.topology.TopologyHarness.ms;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.rahulkumar96t.clickstream.model.PageCount;
import io.github.rahulkumar96t.clickstream.model.TopPages;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TopPagesTest {

  @TempDir Path stateDir;
  private TopologyHarness h;

  @BeforeEach
  void setUp() {
    h = new TopologyHarness(ClickstreamConfig.defaults().withTopN(3), stateDir);
  }

  @AfterEach
  void tearDown() {
    h.close();
  }

  private void views(String page, int n, String time) {
    for (int i = 0; i < n; i++) {
      h.view("user" + i, page, time);
    }
  }

  private static PageCount c(String page, long n) {
    return new PageCount(page, ms("10:00:00"), ms("10:01:00"), n);
  }

  @Test
  void ranksTheTopNPagesOfEachWindow() {
    views("/home", 5, "10:00:10");
    views("/products", 3, "10:00:20");
    views("/cart", 3, "10:00:30"); // ties with /products: page name breaks the tie
    views("/about", 1, "10:00:40");
    views("/help", 2, "10:00:50");
    views("/later", 9, "10:01:10"); // next window; still open afterwards

    h.view("closer", "/x", "10:01:31"); // closes the 10:00 window

    Map<String, TopPages> latest = h.topPages.readKeyValuesToMap();
    assertThat(latest).containsOnlyKeys("2026-01-01T10:00:00Z");
    assertThat(latest.get("2026-01-01T10:00:00Z"))
        .isEqualTo(
            new TopPages(
                ms("10:00:00"),
                ms("10:01:00"),
                List.of(c("/home", 5), c("/cart", 3), c("/products", 3))));
  }

  @Test
  void emitsTheRankingOfEveryClosedWindowAndKeepsItQueryable() {
    views("/a", 2, "10:00:10");
    views("/b", 1, "10:01:10");
    views("/c", 4, "10:01:20");
    h.view("closer", "/x", "10:02:40"); // closes 10:00 and 10:01

    Map<String, TopPages> latest = h.topPages.readKeyValuesToMap();
    assertThat(latest.get("2026-01-01T10:00:00Z").pages())
        .extracting(PageCount::page)
        .containsExactly("/a");
    assertThat(latest.get("2026-01-01T10:01:00Z").pages())
        .extracting(PageCount::page)
        .containsExactly("/c", "/b");

    var store = h.driver.<Long, TopPages>getKeyValueStore(ClickstreamTopology.TOP_PAGES_STORE);
    assertThat(store.get(ms("10:01:00")).pages()).hasSize(2);
  }

  @Test
  void rankingIsIdempotentForRepeatedCounts() {
    TopPages ranking =
        TopPages.emptyWindow(0, 60_000)
            .with(new PageCount("/a", 0, 60_000, 3), 2)
            .with(new PageCount("/b", 0, 60_000, 5), 2)
            .with(new PageCount("/c", 0, 60_000, 1), 2);
    assertThat(ranking.with(new PageCount("/a", 0, 60_000, 3), 2)).isEqualTo(ranking);
    assertThat(ranking.pages()).extracting(PageCount::page).containsExactly("/b", "/a");
  }
}
