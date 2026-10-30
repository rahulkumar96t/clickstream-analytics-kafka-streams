package io.github.rahulkumar96t.clickstream;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AppSettingsTest {

  @Test
  void settingsComeFromTheEnvironment() {
    AppSettings settings =
        AppSettings.fromEnv(
            Map.of("BOOTSTRAP_SERVERS", "kafka:9092", "SESSION_GAP", "PT2M", "TOP_N", "3"));
    assertThat(settings.bootstrapServers()).isEqualTo("kafka:9092");
    assertThat(settings.topology().sessionGap()).isEqualTo(Duration.ofMinutes(2));
    assertThat(settings.topology().topN()).isEqualTo(3);
    assertThat(settings.streamsProperties().getProperty("processing.guarantee"))
        .isEqualTo("exactly_once_v2");
  }

  @Test
  void defaultsSuitALocalSingleBroker() {
    AppSettings settings = AppSettings.fromEnv(Map.of());
    assertThat(settings.bootstrapServers()).isEqualTo("localhost:9092");
    assertThat(settings.replication()).isEqualTo((short) 1);
    assertThat(settings.topology().topics())
        .containsExactly(
            "page-views", "page-view-counts", "top-pages", "user-sessions", "page-views-dlq");
  }
}
