package io.github.rahulkumar96t.clickstream.model;

/**
 * Number of views of one page in one tumbling window.
 *
 * @param windowStart inclusive window start, epoch millis
 * @param windowEnd exclusive window end, epoch millis
 */
public record PageCount(String page, long windowStart, long windowEnd, long count) {}
