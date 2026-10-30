package io.github.rahulkumar96t.clickstream.topology;

import io.github.rahulkumar96t.clickstream.model.PageCount;
import io.github.rahulkumar96t.clickstream.model.PageView;
import io.github.rahulkumar96t.clickstream.model.ParsedPageView;
import io.github.rahulkumar96t.clickstream.model.SessionStats;
import io.github.rahulkumar96t.clickstream.model.TopPages;
import io.github.rahulkumar96t.clickstream.model.UserSession;
import io.github.rahulkumar96t.clickstream.serde.Json;
import io.github.rahulkumar96t.clickstream.serde.ParsedPageViewSerde;
import java.util.Map;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.EmitStrategy;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;

/**
 * Builds the clickstream topology.
 *
 * <pre>
 * page-views (JSON) ── parse ──┬── invalid ──────────────────────────────────────▶ page-views-dlq
 *                              └── valid ──┬── key by page ─▶ 1-min tumbling count (grace) ─▶ page-view-counts
 *                                          │                        └─ key by window ─▶ top-N ─▶ top-pages
 *                                          └── key by user ─▶ 30-min session windows ──────▶ user-sessions
 * </pre>
 *
 * Window counts and sessions are emitted once, when their window closes (event time past the window
 * end plus the grace period), so downstream consumers and the top-N ranking only ever see final
 * values. The underlying state stores are queryable at any time through the REST API.
 */
public final class ClickstreamTopology {

  public static final String PAGE_COUNTS_STORE = "page-counts-store";
  public static final String SESSIONS_STORE = "user-sessions-store";
  public static final String TOP_PAGES_STORE = "top-pages-store";

  private static final Serde<PageView> PAGE_VIEW_SERDE = Json.serde(PageView.class);
  private static final Serde<PageCount> PAGE_COUNT_SERDE = Json.serde(PageCount.class);
  private static final Serde<SessionStats> SESSION_STATS_SERDE = Json.serde(SessionStats.class);
  private static final Serde<UserSession> USER_SESSION_SERDE = Json.serde(UserSession.class);
  private static final Serde<TopPages> TOP_PAGES_SERDE = Json.serde(TopPages.class);

  private ClickstreamTopology() {}

  public static Topology build(ClickstreamConfig config) {
    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, ParsedPageView> parsed =
        builder.stream(
            config.inputTopic(),
            Consumed.with(Serdes.String(), new ParsedPageViewSerde())
                .withTimestampExtractor(new PageViewTimestampExtractor())
                .withName("read-page-views"));

    Map<String, KStream<String, ParsedPageView>> branches =
        parsed
            .split(Named.as("parse-"))
            .branch((key, value) -> value instanceof ParsedPageView.Valid, Branched.as("valid"))
            .defaultBranch(Branched.as("invalid"));

    branches
        .get("parse-invalid")
        .processValues(DeadLetterProcessor::new, Named.as("to-dead-letter"))
        .to(
            config.deadLetterTopic(),
            Produced.with(Serdes.String(), Serdes.ByteArray()).withName("write-dead-letters"));

    KStream<String, PageView> views =
        branches
            .get("parse-valid")
            .mapValues(value -> ((ParsedPageView.Valid) value).event(), Named.as("unwrap-valid"));

    addPageCountsAndTopPages(builder, views, config);
    addSessions(views, config);
    return builder.build();
  }

  private static void addPageCountsAndTopPages(
      StreamsBuilder builder, KStream<String, PageView> views, ClickstreamConfig config) {
    KStream<String, PageCount> finalCounts =
        views
            .selectKey((key, view) -> view.page(), Named.as("key-by-page"))
            .groupByKey(Grouped.with("by-page", Serdes.String(), PAGE_VIEW_SERDE))
            .windowedBy(TimeWindows.ofSizeAndGrace(config.countWindow(), config.lateness()))
            .emitStrategy(EmitStrategy.onWindowClose())
            .count(
                Named.as("count-page-views"),
                Materialized.<String, Long, WindowStore<Bytes, byte[]>>as(PAGE_COUNTS_STORE)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Long())
                    .withRetention(config.countsRetention()))
            .toStream(Named.as("final-page-counts"))
            .map(
                (window, count) ->
                    KeyValue.pair(
                        window.key(),
                        new PageCount(
                            window.key(), window.window().start(), window.window().end(), count)),
                Named.as("to-page-count"));

    finalCounts.to(
        config.pageCountsTopic(),
        Produced.with(Serdes.String(), PAGE_COUNT_SERDE).withName("write-page-counts"));

    builder.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(TOP_PAGES_STORE), Serdes.Long(), TOP_PAGES_SERDE));
    finalCounts
        .selectKey((page, count) -> count.windowStart(), Named.as("key-by-window"))
        .repartition(Repartitioned.with(Serdes.Long(), PAGE_COUNT_SERDE).withName("by-window"))
        .process(
            () -> new TopPagesProcessor(TOP_PAGES_STORE, config.topN(), config.topPagesRetention()),
            Named.as("rank-top-pages"),
            TOP_PAGES_STORE)
        .to(
            config.topPagesTopic(),
            Produced.with(Serdes.String(), TOP_PAGES_SERDE).withName("write-top-pages"));
  }

  private static void addSessions(KStream<String, PageView> views, ClickstreamConfig config) {
    views
        .selectKey((key, view) -> view.userId(), Named.as("key-by-user"))
        .groupByKey(Grouped.with("by-user", Serdes.String(), PAGE_VIEW_SERDE))
        .windowedBy(SessionWindows.ofInactivityGapAndGrace(config.sessionGap(), config.lateness()))
        .emitStrategy(EmitStrategy.onWindowClose())
        .aggregate(
            SessionStats::empty,
            (user, view, stats) -> stats.add(view),
            (user, left, right) -> left.merge(right),
            Named.as("sessionize"),
            Materialized.<String, SessionStats, SessionStore<Bytes, byte[]>>as(SESSIONS_STORE)
                .withKeySerde(Serdes.String())
                .withValueSerde(SESSION_STATS_SERDE)
                .withRetention(config.sessionsRetention()))
        .toStream(Named.as("closed-sessions"))
        .filter((window, stats) -> stats != null, Named.as("skip-merged-sessions"))
        .map(
            (window, stats) ->
                KeyValue.pair(
                    window.key(),
                    UserSession.of(
                        window.key(), window.window().start(), window.window().end(), stats)),
            Named.as("to-user-session"))
        .to(
            config.sessionsTopic(),
            Produced.with(Serdes.String(), USER_SESSION_SERDE).withName("write-sessions"));
  }
}
