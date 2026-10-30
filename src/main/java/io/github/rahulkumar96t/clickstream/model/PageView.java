package io.github.rahulkumar96t.clickstream.model;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * A validated page-view event.
 *
 * @param eventId optional client-side event id
 * @param userId the visitor (required)
 * @param page the page path, e.g. {@code /products/42} (required)
 * @param timestamp event time in epoch milliseconds (required)
 * @param referrer optional referring URL
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record PageView(
    String eventId, String userId, String page, long timestamp, String referrer) {}
