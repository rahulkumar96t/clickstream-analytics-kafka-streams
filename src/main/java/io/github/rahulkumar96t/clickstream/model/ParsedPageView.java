package io.github.rahulkumar96t.clickstream.model;

import java.util.Arrays;

/**
 * Result of parsing a raw input record: either a valid {@link PageView} or the original bytes plus
 * the reason they were rejected. Parsing never throws, so a malformed record cannot crash the
 * stream thread; invalid records are routed to the dead-letter topic instead.
 */
public sealed interface ParsedPageView {

  record Valid(PageView event) implements ParsedPageView {}

  /**
   * @param raw the original record value, unchanged
   * @param reason human-readable validation error
   */
  record Invalid(byte[] raw, String reason) implements ParsedPageView {
    @Override
    public boolean equals(Object o) {
      return o instanceof Invalid other
          && Arrays.equals(raw, other.raw)
          && reason.equals(other.reason);
    }

    @Override
    public int hashCode() {
      return 31 * Arrays.hashCode(raw) + reason.hashCode();
    }

    @Override
    public String toString() {
      return "Invalid[" + reason + ", " + raw.length + " bytes]";
    }
  }
}
