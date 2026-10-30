package io.github.rahulkumar96t.clickstream.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.rahulkumar96t.clickstream.model.SessionStats;
import io.github.rahulkumar96t.clickstream.model.TopPages;
import io.github.rahulkumar96t.clickstream.serde.Json;
import io.github.rahulkumar96t.clickstream.topology.ClickstreamConfig;
import io.github.rahulkumar96t.clickstream.topology.ClickstreamTopology;
import io.github.rahulkumar96t.clickstream.topology.TopologyHarness;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Instant;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The REST API over the TopologyTestDriver's state stores: the same read-only store interfaces a
 * running {@code KafkaStreams} instance hands out for interactive queries.
 */
class QueryServerTest {

  @TempDir Path stateDir;
  private TopologyHarness h;
  private QueryServer server;
  private final HttpClient client = HttpClient.newHttpClient();

  @BeforeEach
  void setUp() throws Exception {
    ClickstreamConfig config = ClickstreamConfig.defaults().withTopN(2);
    h = new TopologyHarness(config, stateDir);
    ClickstreamStores stores =
        new ClickstreamStores() {
          @Override
          public ReadOnlyWindowStore<String, Long> pageCounts() {
            return h.driver.getWindowStore(ClickstreamTopology.PAGE_COUNTS_STORE);
          }

          @Override
          public ReadOnlySessionStore<String, SessionStats> sessions() {
            return h.driver.getSessionStore(ClickstreamTopology.SESSIONS_STORE);
          }

          @Override
          public ReadOnlyKeyValueStore<Long, TopPages> topPages() {
            return h.driver.getKeyValueStore(ClickstreamTopology.TOP_PAGES_STORE);
          }
        };
    server =
        QueryServer.start(
            new InetSocketAddress(InetAddress.getLoopbackAddress(), 0),
            new QueryService(stores, config.countWindow()),
            () -> "RUNNING");

    h.view("alice", "/home", "10:00:05");
    h.view("bob", "/home", "10:00:15");
    h.view("alice", "/cart", "10:00:45");
    h.view("carol", "/about", "10:00:50");
    h.view("alice", "/home", "10:01:40"); // closes the 10:00 window
  }

  @AfterEach
  void tearDown() {
    server.close();
    client.close();
    h.close();
  }

  private HttpResponse<String> get(String pathAndQuery) throws Exception {
    URI uri = URI.create("http://127.0.0.1:" + server.port() + pathAndQuery);
    return client.send(HttpRequest.newBuilder(uri).build(), HttpResponse.BodyHandlers.ofString());
  }

  private JsonNode json(String pathAndQuery) throws Exception {
    HttpResponse<String> response = get(pathAndQuery);
    assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
    assertThat(response.headers().firstValue("Content-Type")).contains("application/json");
    return Json.MAPPER.readTree(response.body());
  }

  @Test
  void pageCountsForOnePageIncludeTheStillOpenWindow() throws Exception {
    JsonNode counts = json("/api/page-counts?page=%2Fhome");
    assertThat(counts).hasSize(2);
    assertThat(counts.get(0).get("windowStart").asLong()).isEqualTo(TopologyHarness.ms("10:00:00"));
    assertThat(counts.get(0).get("count").asLong()).isEqualTo(2);
    assertThat(counts.get(1).get("windowStart").asLong()).isEqualTo(TopologyHarness.ms("10:01:00"));
    assertThat(counts.get(1).get("count").asLong()).isEqualTo(1); // live, not final yet
  }

  @Test
  void pageCountsForAllPagesWithinATimeRange() throws Exception {
    JsonNode counts =
        json("/api/page-counts?from=2026-01-01T10:00:00Z&to=" + TopologyHarness.ms("10:00:30"));
    assertThat(counts)
        .extracting(c -> c.get("page").asText() + "=" + c.get("count").asLong())
        .containsExactly("/home=2", "/about=1", "/cart=1");
  }

  @Test
  void topPagesByWindowAndLatest() throws Exception {
    JsonNode window = json("/api/top-pages?window=2026-01-01T10:00:00Z");
    assertThat(window.get("pages"))
        .extracting(p -> p.get("page").asText())
        .containsExactly("/home", "/about"); // top 2; /about beats /cart on the name tie-break

    JsonNode latest = json("/api/top-pages?limit=5");
    assertThat(latest).hasSize(1);

    assertThat(get("/api/top-pages?window=2026-01-01T11:00:00Z").statusCode()).isEqualTo(404);
  }

  @Test
  void sessionsOfAUser() throws Exception {
    JsonNode sessions = json("/api/users/alice/sessions");
    assertThat(sessions).hasSize(1);
    assertThat(sessions.get(0).get("pageViews").asLong()).isEqualTo(3);
    assertThat(sessions.get(0).get("entryPage").asText()).isEqualTo("/home");
    assertThat(sessions.get(0).get("exitPage").asText()).isEqualTo("/home");
    assertThat(json("/api/users/nobody/sessions")).isEmpty();
  }

  @Test
  void reportsHealthAndClientErrors() throws Exception {
    assertThat(json("/health").get("state").asText()).isEqualTo("RUNNING");
    assertThat(get("/api/page-counts?from=yesterday").statusCode()).isEqualTo(400);
    assertThat(get("/api/top-pages?limit=0").statusCode()).isEqualTo(400);
    assertThat(get("/api/users/alice/other").statusCode()).isEqualTo(404);
    HttpResponse<String> post =
        client.send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.port() + "/health"))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build(),
            HttpResponse.BodyHandlers.ofString());
    assertThat(post.statusCode()).isEqualTo(405);
  }

  @Test
  void unavailableStoresYield503() throws Exception {
    ClickstreamStores rebalancing =
        new ClickstreamStores() {
          @Override
          public ReadOnlyWindowStore<String, Long> pageCounts() {
            throw new InvalidStateStoreException("rebalancing");
          }

          @Override
          public ReadOnlySessionStore<String, SessionStats> sessions() {
            throw new InvalidStateStoreException("rebalancing");
          }

          @Override
          public ReadOnlyKeyValueStore<Long, TopPages> topPages() {
            throw new InvalidStateStoreException("rebalancing");
          }
        };
    try (QueryServer unavailable =
        QueryServer.start(
            new InetSocketAddress(InetAddress.getLoopbackAddress(), 0),
            new QueryService(rebalancing, java.time.Duration.ofMinutes(1)),
            () -> "REBALANCING")) {
      URI uri = URI.create("http://127.0.0.1:" + unavailable.port() + "/api/page-counts");
      HttpResponse<String> response =
          client.send(HttpRequest.newBuilder(uri).build(), HttpResponse.BodyHandlers.ofString());
      assertThat(response.statusCode()).isEqualTo(503);
    }
  }

  @Test
  void parsesTimesAsIsoOrEpochMillis() {
    assertThat(QueryServer.instant("1767261600000", null))
        .isEqualTo(Instant.parse("2026-01-01T10:00:00Z"));
    assertThat(QueryServer.instant("2026-01-01T10:00:00Z", null))
        .isEqualTo(Instant.ofEpochMilli(1767261600000L));
    assertThatThrownBy(() -> QueryServer.instant(null, null))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
