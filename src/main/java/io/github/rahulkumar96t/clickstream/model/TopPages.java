package io.github.rahulkumar96t.clickstream.model;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** The most viewed pages of one tumbling window, highest count first (ties broken by page name). */
public record TopPages(long windowStart, long windowEnd, List<PageCount> pages) {

  /** Ranking order: count descending, then page ascending for a stable, deterministic order. */
  public static final Comparator<PageCount> RANKING =
      Comparator.comparingLong(PageCount::count).reversed().thenComparing(PageCount::page);

  public TopPages {
    pages = List.copyOf(pages);
  }

  public static TopPages emptyWindow(long windowStart, long windowEnd) {
    return new TopPages(windowStart, windowEnd, List.of());
  }

  /**
   * Inserts (or replaces) the final count of one page and keeps the best {@code n}. Replacing makes
   * the operation idempotent when a count is delivered twice.
   */
  public TopPages with(PageCount count, int n) {
    List<PageCount> ranked = new ArrayList<>(pages.size() + 1);
    for (PageCount existing : pages) {
      if (!existing.page().equals(count.page())) {
        ranked.add(existing);
      }
    }
    ranked.add(count);
    ranked.sort(RANKING);
    return new TopPages(windowStart, windowEnd, ranked.subList(0, Math.min(n, ranked.size())));
  }
}
