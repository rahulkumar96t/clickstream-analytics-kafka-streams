package io.github.rahulkumar96t.clickstream.model;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rahulkumar96t.clickstream.serde.Json;
import io.github.rahulkumar96t.clickstream.serde.PageViewParser;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

class ModelTest {

  private static PageView view(String page, long ts) {
    return new PageView(null, "u", page, ts, null);
  }

  @Test
  void parsesEpochMillisAndIsoTimestampsAndIgnoresUnknownFields() {
    ParsedPageView epoch =
        PageViewParser.parse(
            "{\"userId\":\"u1\",\"page\":\"/a\",\"timestamp\":1767261600000,\"extra\":true}"
                .getBytes(StandardCharsets.UTF_8));
    ParsedPageView iso =
        PageViewParser.parse(
            "{\"userId\":\" u1 \",\"page\":\"/a\",\"timestamp\":\"2026-01-01T10:00:00Z\",\"referrer\":\"r\"}"
                .getBytes(StandardCharsets.UTF_8));

    assertThat(epoch)
        .isEqualTo(new ParsedPageView.Valid(new PageView(null, "u1", "/a", 1767261600000L, null)));
    assertThat(iso)
        .isEqualTo(new ParsedPageView.Valid(new PageView(null, "u1", "/a", 1767261600000L, "r")));
  }

  @Test
  void rejectsOversizedFields() {
    String longId = "x".repeat(300);
    ParsedPageView parsed =
        PageViewParser.parse(
            ("{\"userId\":\"" + longId + "\",\"page\":\"/a\",\"timestamp\":1}")
                .getBytes(StandardCharsets.UTF_8));
    assertThat(parsed).isInstanceOf(ParsedPageView.Invalid.class);
  }

  @Test
  void sessionStatsTrackEntryAndExitPagesRegardlessOfArrivalOrder() {
    SessionStats stats =
        SessionStats.empty()
            .add(view("/middle", 20))
            .add(view("/last", 30))
            .add(view("/first", 10));
    assertThat(stats).isEqualTo(new SessionStats(3, 10, 30, "/first", "/last"));
  }

  @Test
  void sessionStatsMergeIsCommutativeAndHasAnIdentity() {
    SessionStats early = SessionStats.empty().add(view("/a", 10)).add(view("/b", 20));
    SessionStats late = SessionStats.empty().add(view("/c", 40));
    SessionStats merged = new SessionStats(3, 10, 40, "/a", "/c");

    assertThat(early.merge(late)).isEqualTo(merged);
    assertThat(late.merge(early)).isEqualTo(merged);
    assertThat(SessionStats.empty().merge(early)).isEqualTo(early);
    assertThat(early.merge(SessionStats.empty())).isEqualTo(early);
  }

  @Test
  void topPagesKeepsTheBestNInRankingOrder() {
    TopPages top = TopPages.emptyWindow(0, 60_000);
    for (String page : List.of("/a", "/b", "/c", "/d")) {
      top = top.with(new PageCount(page, 0, 60_000, page.equals("/c") ? 1 : 7), 3);
    }
    assertThat(top.pages()).extracting(PageCount::page).containsExactly("/a", "/b", "/d");
  }

  @Test
  void recordsRoundTripThroughJson() {
    var serde = Json.serde(TopPages.class);
    TopPages top = new TopPages(0, 60_000, List.of(new PageCount("/a", 0, 60_000, 3)));
    assertThat(serde.deserializer().deserialize("t", serde.serializer().serialize("t", top)))
        .isEqualTo(top);
    assertThat(Json.write(new PageView(null, "u", "/a", 1, null)))
        .isEqualTo("{\"userId\":\"u\",\"page\":\"/a\",\"timestamp\":1}");
  }
}
