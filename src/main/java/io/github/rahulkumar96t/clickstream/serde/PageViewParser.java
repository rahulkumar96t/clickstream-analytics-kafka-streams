package io.github.rahulkumar96t.clickstream.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.github.rahulkumar96t.clickstream.model.PageView;
import io.github.rahulkumar96t.clickstream.model.ParsedPageView;
import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeParseException;

/**
 * Parses and validates raw page-view JSON. Never throws: every problem becomes a {@link
 * ParsedPageView.Invalid} with a reason that ends up in the dead-letter record's headers.
 *
 * <pre>
 * {"userId": "u-17", "page": "/products/42", "timestamp": 1767225600000,
 *  "eventId": "optional", "referrer": "optional"}
 * </pre>
 *
 * {@code timestamp} is epoch milliseconds or an ISO-8601 instant such as {@code
 * "2026-01-01T00:00:00Z"}. Unknown fields are ignored.
 */
public final class PageViewParser {

  static final int MAX_ID_LENGTH = 256;
  static final int MAX_PAGE_LENGTH = 2048;

  private PageViewParser() {}

  public static ParsedPageView parse(byte[] raw) {
    if (raw == null || raw.length == 0) {
      return new ParsedPageView.Invalid(raw == null ? new byte[0] : raw, "empty value");
    }
    JsonNode node;
    try {
      node = Json.MAPPER.readTree(raw);
    } catch (JsonProcessingException e) {
      return invalid(raw, "malformed JSON: " + e.getOriginalMessage());
    } catch (IOException e) {
      return invalid(raw, "unreadable value: " + e.getMessage());
    }
    if (node == null || !node.isObject()) {
      return invalid(raw, "expected a JSON object");
    }
    String userId = text(node, "userId", MAX_ID_LENGTH);
    if (userId == null) {
      return invalid(raw, "missing or invalid field 'userId'");
    }
    String page = text(node, "page", MAX_PAGE_LENGTH);
    if (page == null) {
      return invalid(raw, "missing or invalid field 'page'");
    }
    Long timestamp = timestamp(node.get("timestamp"));
    if (timestamp == null) {
      return invalid(raw, "missing or invalid field 'timestamp' (epoch millis or ISO-8601)");
    }
    return new ParsedPageView.Valid(
        new PageView(
            text(node, "eventId", MAX_ID_LENGTH),
            userId,
            page,
            timestamp,
            text(node, "referrer", MAX_PAGE_LENGTH)));
  }

  private static ParsedPageView invalid(byte[] raw, String reason) {
    return new ParsedPageView.Invalid(raw, reason);
  }

  /** A non-blank string field of bounded length, or {@code null}. */
  private static String text(JsonNode node, String field, int maxLength) {
    JsonNode value = node.get(field);
    if (value == null || !value.isTextual()) {
      return null;
    }
    String text = value.textValue().strip();
    return text.isEmpty() || text.length() > maxLength ? null : text;
  }

  private static Long timestamp(JsonNode value) {
    if (value == null) {
      return null;
    }
    if (value.isIntegralNumber() && value.canConvertToLong() && value.longValue() >= 0) {
      return value.longValue();
    }
    if (value.isTextual()) {
      try {
        long millis = Instant.parse(value.textValue()).toEpochMilli();
        return millis >= 0 ? millis : null;
      } catch (DateTimeParseException | ArithmeticException e) {
        return null;
      }
    }
    return null;
  }
}
