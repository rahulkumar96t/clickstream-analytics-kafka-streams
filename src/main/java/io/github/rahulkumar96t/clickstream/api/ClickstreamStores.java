package io.github.rahulkumar96t.clickstream.api;

import io.github.rahulkumar96t.clickstream.model.SessionStats;
import io.github.rahulkumar96t.clickstream.model.TopPages;
import io.github.rahulkumar96t.clickstream.topology.ClickstreamTopology;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;

/**
 * Read-only access to the topology's state stores. In production the stores come from {@link
 * KafkaStreams#store} (interactive queries); tests hand in the stores of a {@code
 * TopologyTestDriver}, which implement the same read-only interfaces.
 */
public interface ClickstreamStores {

  ReadOnlyWindowStore<String, Long> pageCounts();

  ReadOnlySessionStore<String, SessionStats> sessions();

  ReadOnlyKeyValueStore<Long, TopPages> topPages();

  /** Stores of a running application. Lookups fail while the instance is (re)balancing. */
  static ClickstreamStores of(KafkaStreams streams) {
    return new ClickstreamStores() {
      @Override
      public ReadOnlyWindowStore<String, Long> pageCounts() {
        return streams.store(
            StoreQueryParameters.fromNameAndType(
                ClickstreamTopology.PAGE_COUNTS_STORE, QueryableStoreTypes.windowStore()));
      }

      @Override
      public ReadOnlySessionStore<String, SessionStats> sessions() {
        return streams.store(
            StoreQueryParameters.fromNameAndType(
                ClickstreamTopology.SESSIONS_STORE, QueryableStoreTypes.sessionStore()));
      }

      @Override
      public ReadOnlyKeyValueStore<Long, TopPages> topPages() {
        return streams.store(
            StoreQueryParameters.fromNameAndType(
                ClickstreamTopology.TOP_PAGES_STORE, QueryableStoreTypes.keyValueStore()));
      }
    };
  }
}
