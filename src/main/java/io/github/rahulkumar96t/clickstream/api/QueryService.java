package io.github.rahulkumar96t.clickstream.api;

import io.github.rahulkumar96t.clickstream.model.PageCount;
import io.github.rahulkumar96t.clickstream.model.SessionStats;
import io.github.rahulkumar96t.clickstream.model.TopPages;
import io.github.rahulkumar96t.clickstream.model.UserSession;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStoreIterator;

/**
 * Queries over the local state stores. Window counts and sessions include windows that are still
 * open, so the API shows live values while the output topics only carry final ones.
 */
public final class QueryService {

  private final ClickstreamStores stores;
  private final long windowSizeMs;

  public QueryService(ClickstreamStores stores, Duration countWindow) {
    this.stores = stores;
    this.windowSizeMs = countWindow.toMillis();
  }

  /** Counts for one page, for windows starting within {@code [from, to]}. */
  public List<PageCount> pageCounts(String page, Instant from, Instant to) {
    List<PageCount> result = new ArrayList<>();
    try (WindowStoreIterator<Long> it = stores.pageCounts().fetch(page, from, to)) {
      while (it.hasNext()) {
        KeyValue<Long, Long> entry = it.next();
        result.add(new PageCount(page, entry.key, entry.key + windowSizeMs, entry.value));
      }
    }
    return result;
  }

  /** Counts for every page, for windows starting within {@code [from, to]}, busiest first. */
  public List<PageCount> allPageCounts(Instant from, Instant to) {
    List<PageCount> result = new ArrayList<>();
    try (KeyValueIterator<Windowed<String>, Long> it = stores.pageCounts().fetchAll(from, to)) {
      while (it.hasNext()) {
        KeyValue<Windowed<String>, Long> entry = it.next();
        result.add(
            new PageCount(
                entry.key.key(),
                entry.key.window().start(),
                entry.key.window().end(),
                entry.value));
      }
    }
    result.sort(Comparator.comparingLong(PageCount::windowStart).thenComparing(TopPages.RANKING));
    return result;
  }

  /** The ranking for the window starting at {@code windowStart}, once any of it is final. */
  public Optional<TopPages> topPages(Instant windowStart) {
    return Optional.ofNullable(stores.topPages().get(windowStart.toEpochMilli()));
  }

  /** The {@code limit} most recent windows that have a ranking, newest first. */
  public List<TopPages> latestTopPages(int limit) {
    List<TopPages> result = new ArrayList<>();
    try (KeyValueIterator<Long, TopPages> it = stores.topPages().reverseAll()) {
      while (it.hasNext() && result.size() < limit) {
        result.add(it.next().value);
      }
    }
    return result;
  }

  /** Sessions of one user known to this instance (open and recently closed), oldest first. */
  public List<UserSession> sessions(String userId) {
    List<UserSession> result = new ArrayList<>();
    try (KeyValueIterator<Windowed<String>, SessionStats> it = stores.sessions().fetch(userId)) {
      while (it.hasNext()) {
        KeyValue<Windowed<String>, SessionStats> entry = it.next();
        result.add(
            UserSession.of(
                userId, entry.key.window().start(), entry.key.window().end(), entry.value));
      }
    }
    return result;
  }
}
