package io.github.rahulkumar96t.clickstream.model;

/**
 * A closed (or, when served by the query API, possibly still open) user session.
 *
 * @param sessionStart timestamp of the first view, epoch millis
 * @param sessionEnd timestamp of the last view, epoch millis
 * @param durationMs {@code sessionEnd - sessionStart}
 */
public record UserSession(
    String userId,
    long sessionStart,
    long sessionEnd,
    long durationMs,
    long pageViews,
    String entryPage,
    String exitPage) {

  public static UserSession of(String userId, long start, long end, SessionStats stats) {
    return new UserSession(
        userId, start, end, end - start, stats.pageViews(), stats.entryPage(), stats.exitPage());
  }
}
