package io.github.rahulkumar96t.clickstream.topology;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class DeadLetterTest {

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

  private static String header(Headers headers, String name) {
    Header header = headers.lastHeader(name);
    return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
  }

  static Stream<Arguments> malformedRecords() {
    String noUser = "missing or invalid field 'userId'";
    String noPage = "missing or invalid field 'page'";
    String noTime = "missing or invalid field 'timestamp'";
    return Stream.of(
        Arguments.of("this is not json", "malformed JSON"),
        Arguments.of("[1, 2, 3]", "expected a JSON object"),
        Arguments.of("{\"page\":\"/x\",\"timestamp\":1}", noUser),
        Arguments.of("{\"userId\":\"  \",\"page\":\"/x\",\"timestamp\":1}", noUser),
        Arguments.of("{\"userId\":\"u\",\"timestamp\":1}", noPage),
        Arguments.of("{\"userId\":\"u\",\"page\":7,\"timestamp\":1}", noPage),
        Arguments.of("{\"userId\":\"u\",\"page\":\"/x\"}", noTime),
        Arguments.of("{\"userId\":\"u\",\"page\":\"/x\",\"timestamp\":\"soon\"}", noTime),
        Arguments.of("{\"userId\":\"u\",\"page\":\"/x\",\"timestamp\":-5}", noTime),
        Arguments.of("{\"userId\":\"u\",\"page\":\"/x\",\"timestamp\":1.5}", noTime),
        Arguments.of("{\"userId\":\"u\",\"page\":\"/x\",\"timestamp\":1} {}", "malformed JSON"));
  }

  @ParameterizedTest
  @MethodSource("malformedRecords")
  void malformedRecordsGoToTheDeadLetterTopicUnchangedWithTheReason(String raw, String reason) {
    h.raw("key-1", raw);

    TestRecord<String, byte[]> dead = h.deadLetters.readRecord();
    assertThat(dead.key()).isEqualTo("key-1");
    assertThat(new String(dead.value(), StandardCharsets.UTF_8)).isEqualTo(raw);
    assertThat(header(dead.headers(), DeadLetterProcessor.REASON_HEADER)).startsWith(reason);
    assertThat(header(dead.headers(), DeadLetterProcessor.TOPIC_HEADER)).isEqualTo("page-views");
    assertThat(header(dead.headers(), DeadLetterProcessor.PARTITION_HEADER)).isEqualTo("0");
    assertThat(header(dead.headers(), DeadLetterProcessor.OFFSET_HEADER)).isNotNull();
    assertThat(h.deadLetters.isEmpty()).isTrue();
  }

  @Test
  void emptyValuesAreDeadLettered() {
    h.input.pipeInput("key-2", (String) null);
    TestRecord<String, byte[]> dead = h.deadLetters.readRecord();
    assertThat(header(dead.headers(), DeadLetterProcessor.REASON_HEADER)).isEqualTo("empty value");
  }

  @Test
  void badRecordsDoNotDisturbValidOnes() {
    h.view("u1", "/home", "10:00:00");
    h.raw("x", "garbage");
    h.view("u2", "/home", "10:00:30");
    h.raw("y", "{}");
    h.view("u3", "/z", "10:02:00");

    assertThat(h.deadLetters.readValuesToList()).hasSize(2);
    assertThat(h.pageCounts.readValuesToList())
        .singleElement()
        .satisfies(count -> assertThat(count.count()).isEqualTo(2));
  }

  @Test
  void originalHeadersAreKept() {
    h.input.pipeInput(
        new TestRecord<>(
            "k",
            "nope",
            new org.apache.kafka.common.header.internals.RecordHeaders()
                .add("trace-id", "abc".getBytes(StandardCharsets.UTF_8)),
            TopologyHarness.t("00:00:00")));
    TestRecord<String, byte[]> dead = h.deadLetters.readRecord();
    assertThat(header(dead.headers(), "trace-id")).isEqualTo("abc");
  }
}
