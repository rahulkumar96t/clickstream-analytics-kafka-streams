package io.github.rahulkumar96t.clickstream.topology;

import static io.github.rahulkumar96t.clickstream.topology.TopologyHarness.ms;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.rahulkumar96t.clickstream.model.UserSession;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 30-minute inactivity sessions with 30 s of grace, emitted once when they close. */
class SessionizationTest {

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

  private static UserSession session(
      String user, String start, String end, long views, String entry, String exit) {
    return new UserSession(user, ms(start), ms(end), ms(end) - ms(start), views, entry, exit);
  }

  @Test
  void viewsWithinTheGapFormOneSessionEmittedAfterInactivity() {
    h.view("alice", "/home", "10:00:00");
    h.view("alice", "/products", "10:10:00");
    h.view("alice", "/cart", "10:25:00");

    // Closes once stream time passes 10:25 + 30 min gap + 30 s grace.
    h.view("bob", "/home", "10:55:00");
    assertThat(h.sessions.isEmpty()).isTrue();
    h.view("bob", "/home", "10:56:00");

    assertThat(h.sessions.readKeyValuesToList())
        .singleElement()
        .satisfies(
            kv -> {
              assertThat(kv.key).isEqualTo("alice");
              assertThat(kv.value)
                  .isEqualTo(session("alice", "10:00:00", "10:25:00", 3, "/home", "/cart"));
              assertThat(kv.value.durationMs()).isEqualTo(Duration.ofMinutes(25).toMillis());
            });
  }

  @Test
  void aGapLongerThanTheInactivityThresholdStartsANewSession() {
    h.view("alice", "/a", "10:00:00");
    h.view("alice", "/b", "10:31:00"); // 31 minutes later
    h.view("bob", "/x", "12:00:00"); // closes both

    assertThat(h.sessions.readValuesToList())
        .containsExactlyInAnyOrder(
            session("alice", "10:00:00", "10:00:00", 1, "/a", "/a"),
            session("alice", "10:31:00", "10:31:00", 1, "/b", "/b"));
  }

  @Test
  void anOutOfOrderEventBridgingTwoOpenSessionsMergesThem() {
    h.view("alice", "/a", "10:00:00");
    h.view("alice", "/c", "10:30:10"); // 30m10s later: a separate session so far
    h.view("alice", "/b", "10:15:00"); // out of order; both sessions are still open

    h.view("bob", "/x", "12:00:00");

    assertThat(h.sessions.readValuesToList())
        .containsExactly(session("alice", "10:00:00", "10:30:10", 3, "/a", "/c"));
  }

  @Test
  void aLateEventCanStillMergeIntoASessionThatWasAlreadyEmitted() {
    // Kafka Streams accepts a late event as long as the session it merges into has not closed.
    // Here the first session [10:00] was already emitted at stream time 10:40, yet the 10:20
    // event bridges it with the open [10:40] session. The merged session is emitted later with
    // the same user and start time, so consumers should upsert by (userId, sessionStart).
    h.view("alice", "/a", "10:00:00");
    h.view("alice", "/c", "10:40:00");
    assertThat(h.sessions.readValuesToList())
        .containsExactly(session("alice", "10:00:00", "10:00:00", 1, "/a", "/a"));

    h.view("alice", "/b", "10:20:00");
    h.view("bob", "/x", "12:00:00");

    assertThat(h.sessions.readValuesToList())
        .containsExactly(session("alice", "10:00:00", "10:40:00", 3, "/a", "/c"));
  }

  @Test
  void sessionsArePerUserAndOpenOnesAreQueryable() {
    h.view("alice", "/a", "10:00:00");
    h.view("bob", "/b", "10:05:00");
    h.view("alice", "/c", "10:06:00");

    var store = h.driver.<String, Object>getSessionStore(ClickstreamTopology.SESSIONS_STORE);
    try (var alice = store.fetch("alice")) {
      assertThat(alice.next().key.window().end()).isEqualTo(ms("10:06:00"));
      assertThat(alice.hasNext()).isFalse();
    }
    assertThat(h.sessions.isEmpty()).isTrue();
  }
}
