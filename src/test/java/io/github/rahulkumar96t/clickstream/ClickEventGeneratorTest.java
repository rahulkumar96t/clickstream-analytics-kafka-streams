package io.github.rahulkumar96t.clickstream;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rahulkumar96t.clickstream.model.ParsedPageView;
import io.github.rahulkumar96t.clickstream.serde.PageViewParser;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class ClickEventGeneratorTest {

  @Test
  void producesMostlyValidEventsWithTheConfiguredShareOfLateAndMalformedOnes() {
    ClickEventGenerator generator =
        new ClickEventGenerator(7, 50, 0.10, 0.05, Duration.ofMinutes(2));
    long now = 1_767_261_600_000L;
    int valid = 0;
    int late = 0;
    int malformed = 0;
    for (int i = 0; i < 10_000; i++) {
      ClickEventGenerator.Event event = generator.next(now + i * 100L);
      ParsedPageView parsed = PageViewParser.parse(event.value().getBytes(StandardCharsets.UTF_8));
      if (parsed instanceof ParsedPageView.Valid v) {
        valid++;
        assertThat(event.key()).isEqualTo(v.event().userId());
        if (v.event().timestamp() < now + i * 100L) {
          late++;
        }
      } else {
        malformed++;
      }
    }
    assertThat(malformed).isBetween(400, 600); // ~5%
    assertThat(late).isBetween(valid / 10 - 150, valid / 10 + 150); // ~10% of valid events
  }

  @Test
  void isDeterministicForASeed() {
    ClickEventGenerator a = new ClickEventGenerator(1, 10, 0.1, 0.1, Duration.ofMinutes(1));
    ClickEventGenerator b = new ClickEventGenerator(1, 10, 0.1, 0.1, Duration.ofMinutes(1));
    for (int i = 0; i < 100; i++) {
      assertThat(a.next(i * 1000L)).isEqualTo(b.next(i * 1000L));
    }
  }
}
