package io.github.rahulkumar96t.clickstream.model;

/**
 * Running aggregate of one user session (the state stored per session window).
 *
 * @param pageViews number of page views in the session
 * @param firstSeen timestamp of the earliest view, epoch millis
 * @param lastSeen timestamp of the latest view, epoch millis
 * @param entryPage page of the earliest view
 * @param exitPage page of the latest view
 */
public record SessionStats(
    long pageViews, long firstSeen, long lastSeen, String entryPage, String exitPage) {

  public static SessionStats empty() {
    return new SessionStats(0, Long.MAX_VALUE, Long.MIN_VALUE, null, null);
  }

  /** Adds one view; events may arrive out of order. */
  public SessionStats add(PageView view) {
    long t = view.timestamp();
    return new SessionStats(
        pageViews + 1,
        Math.min(firstSeen, t),
        Math.max(lastSeen, t),
        t < firstSeen ? view.page() : entryPage,
        t >= lastSeen ? view.page() : exitPage);
  }

  /** Combines two sessions that a late event has bridged into one. */
  public SessionStats merge(SessionStats other) {
    if (other.pageViews == 0) {
      return this;
    }
    if (pageViews == 0) {
      return other;
    }
    return new SessionStats(
        pageViews + other.pageViews,
        Math.min(firstSeen, other.firstSeen),
        Math.max(lastSeen, other.lastSeen),
        firstSeen <= other.firstSeen ? entryPage : other.entryPage,
        lastSeen >= other.lastSeen ? exitPage : other.exitPage);
  }
}
